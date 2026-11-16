package ru.nsu.klochikhina.raft.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpStatus;
import org.mockito.ArgumentCaptor;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.*;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.model.NodeInfo;
import ru.nsu.klochikhina.raft.model.NodeStatus;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static ru.nsu.klochikhina.raft.support.RaftTestFixture.*;

class RaftServiceTests {

    private RaftNode raftNode;
    private ReplicationService replicationService;
    private RaftService service;
    private ApplyService applyService;

    @BeforeEach
    void setUp() {
        raftNode = node(3);
        raftNode.getClusterProperties().setCommitTimeout(100);
        replicationService = mock(ReplicationService.class);
        applyService = application(raftNode);
        service = new RaftService(raftNode, replicationService, applyService);
    }

    @Test
    void leaderAppendsWholeBatchWithItsCurrentTermAndStartsReplicationOutsideLock() {
        raftNode.getLogEntries().add(new LogEntry(1, 1, "old"));
        raftNode.getCurrentTerm().set(1);
        raftNode.setCommitIndex(1);
        electLeader(raftNode);
        doAnswer(_ -> {
            assertFalse(Thread.holdsLock(raftNode));
            assertEquals(3, raftNode.getLastLogIndex());
            raftNode.setCommitIndex(3);
            applyService.applyCommittedEntries();
            return null;
        }).when(replicationService).replicateLog();

        AddEntriesResponse response = service.addEntries(new AddEntries(List.of("first", "second")));

        assertEquals(appliedResponse(2, "first", "second"), response);
        assertEquals(List.of(new LogEntry(1, 1, "old"), new LogEntry(2, 2, "first"), new LogEntry(3, 2, "second")),
                raftNode.getLogEntries());
        assertEquals(3, raftNode.getCommitIndex());
        verify(replicationService).replicateLog();
    }

    @ParameterizedTest
    @EnumSource(value = NodeStatus.class, names = {"FOLLOWER", "CANDIDATE"})
    void nonLeaderRejectsWritesWithoutChangingLog(NodeStatus status) {
        raftNode.setStatus(status);

        assertEquals(new AddEntriesResponse(HttpStatus.CONFLICT, false, false, List.of()),
                service.addEntries(new AddEntries(List.of("command"))));
        assertTrue(raftNode.getLogEntries().isEmpty());
        verifyNoInteractions(replicationService);
    }

    @ParameterizedTest
    @MethodSource("invalidBatches")
    void invalidBatchIsRejectedWithoutPartialAppend(AddEntries request) {
        electLeader(raftNode);
        raftNode.getLogEntries().add(new LogEntry(1, 1, "existing"));

        assertEquals(new AddEntriesResponse(HttpStatus.BAD_REQUEST, false, false, List.of()), service.addEntries(request));
        assertEquals(List.of(new LogEntry(1, 1, "existing")), raftNode.getLogEntries());
        verifyNoInteractions(replicationService);
    }

    static Stream<AddEntries> invalidBatches() {
        return Stream.of(null, new AddEntries(null), new AddEntries(List.of()),
                new AddEntries(Arrays.asList("valid", null)));
    }

    @Test
    void commandsAreStoredVerbatim() {
        electLeader(raftNode);
        List<String> commands = List.of("", "  spaced  ", "ключ=значение", "same", "same");
        doAnswer(_ -> {
            raftNode.setCommitIndex(raftNode.getLastLogIndex());
            applyService.applyCommittedEntries();
            return null;
        }).when(replicationService).replicateLog();

        assertEquals(appliedResponse(1, commands.toArray(String[]::new)), service.addEntries(new AddEntries(commands)));

        assertEquals(commands, service.status().entries().stream().map(LogEntry::command).toList());
    }

    @Test
    void singleNodeClusterCanCommitBeforeReturning() {
        RaftNode singleNode = node(1);
        electLeader(singleNode);
        NetworkService network = mock(NetworkService.class);
        ApplyService singleApplication = application(singleNode);
        RaftService singleNodeService = new RaftService(singleNode,
                new ReplicationService(singleNode, network, singleApplication), singleApplication);

        assertEquals(appliedResponse(1, "A", "B"),
                singleNodeService.addEntries(new AddEntries(List.of("A", "B"))));
        assertEquals(2, singleNode.getCommitIndex());
        verifyNoInteractions(network);
    }

    @Test
    void replacedEntryIsNotReportedAsCommittedAfterLeadershipChanges() {
        electLeader(raftNode);
        doAnswer(_ -> {
            raftNode.becomeFollower(2);
            raftNode.getLogEntries().clear();
            raftNode.getLogEntries().add(new LogEntry(1, 2, "replacement"));
            raftNode.setCommitIndex(1);
            return null;
        }).when(replicationService).replicateLog();

        assertEquals(new AddEntriesResponse(HttpStatus.CONFLICT, false, false, List.of()),
                service.addEntries(new AddEntries(List.of("original"))));
    }

    @Test
    void statusReturnsImmutableSnapshotWithCommitIndex() {
        raftNode.getLogEntries().addAll(List.of(new LogEntry(1, 1, "A"), new LogEntry(2, 2, "B")));
        raftNode.getCurrentTerm().set(2);
        raftNode.setCommitIndex(1);

        StatusResponse snapshot = service.status();
        raftNode.getLogEntries().clear();
        raftNode.setCommitIndex(0);
        raftNode.becomeCandidate();

        assertEquals(List.of(new LogEntry(1, 1, "A"), new LogEntry(2, 2, "B")), snapshot.entries());
        assertEquals(1, snapshot.commitIndex());
        assertEquals(2, snapshot.currentTerm());
        assertEquals(NodeStatus.FOLLOWER, snapshot.status());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.entries().add(new LogEntry(3, 3, "C")));
    }

    @Test
    void emptyLogCanBeReadFromFollower() {
        StatusResponse snapshot = service.status();
        assertEquals(List.of(), snapshot.entries());
        assertEquals(0, snapshot.commitIndex());
    }

    @Test
    void statusReportsSelfRoleTermAndOtherNodes() {
        electLeader(raftNode);

        assertEquals(new StatusResponse(new NodeInfo(1, 8081, "localhost"), NodeStatus.LEADER, 1,
                List.of(new NodeInfo(2, 8082, "localhost"), new NodeInfo(3, 8083, "localhost")),
                List.of(), 0, 0), service.status());
    }

    @Test
    void statusHasFallbackWhenSelfIsMissingFromClusterConfig() {
        raftNode.getClusterProperties().setNodes(List.of(new NodeInfo(2, 8082, "localhost")));

        assertEquals(new NodeInfo(1, 8081, "unknown"), service.status().nodeInfo());
    }

    @Test
    void responseWaitsForMajorityAndCommitBoundaryIsPropagatedToFollower() throws Exception {
        electLeader(raftNode);
        raftNode.getClusterProperties().setCommitTimeout(5000);
        NetworkService network = mock(NetworkService.class);
        ReplicationService replication = new ReplicationService(raftNode, network, applyService);
        RaftService leaderService = new RaftService(raftNode, replication, applyService);
        RaftNode follower = node(2, 3);
        RaftService followerService = new RaftService(follower, mock(ReplicationService.class), application(follower));
        NodeInfo peer = raftNode.getClusterProperties().getNodes().get(1);
        CompletableFuture<AppendEntriesResponse> appendResponse = new CompletableFuture<>();
        CompletableFuture<AppendEntriesResponse> heartbeatResponse = new CompletableFuture<>();
        CountDownLatch sent = new CountDownLatch(1);
        when(network.sendAppendEntries(eq(peer), any())).thenAnswer(_ -> {
            sent.countDown();
            return appendResponse;
        }).thenReturn(heartbeatResponse);
        when(network.sendAppendEntries(eq(raftNode.getClusterProperties().getNodes().get(2)), any()))
                .thenReturn(new CompletableFuture<>());

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var response = executor.submit(() -> leaderService.addEntries(new AddEntries(List.of("A", "B"))));
            assertTrue(sent.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> response.get(50, TimeUnit.MILLISECONDS));
            // Если ожидание удерживает монитор узла, этот запрос status не завершится.
            assertEquals(0, executor.submit(leaderService::status).get(1, TimeUnit.SECONDS).commitIndex());

            ArgumentCaptor<AppendEntries> requests = ArgumentCaptor.forClass(AppendEntries.class);
            verify(network).sendAppendEntries(eq(peer), requests.capture());
            appendResponse.complete(followerService.appendEntries(requests.getValue()));
            assertEquals(appliedResponse(1, "A", "B"), response.get(1, TimeUnit.SECONDS));

            assertEquals(2, leaderService.status().commitIndex());
            assertEquals(0, followerService.status().commitIndex());
            replication.replicateLog();
            verify(network, times(2)).sendAppendEntries(eq(peer), requests.capture());
            heartbeatResponse.complete(followerService.appendEntries(requests.getValue()));

            StatusResponse leaderStatus = leaderService.status();
            StatusResponse followerStatus = followerService.status();
            assertEquals(leaderStatus.entries(), followerStatus.entries());
            assertEquals(leaderStatus.commitIndex(), followerStatus.commitIndex());
            assertEquals(List.of(new LogEntry(1, 1, "A"), new LogEntry(2, 1, "B")), followerStatus.entries());
        }
    }

    @Test
    void batchRemainsUncommittedWhenNoFollowerConfirmsIt() {
        electLeader(raftNode);
        NetworkService network = mock(NetworkService.class);
        when(network.sendAppendEntries(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new AppendEntriesResponse(false, 1)));
        RaftService leaderService = new RaftService(raftNode,
                new ReplicationService(raftNode, network, applyService), applyService);

        assertEquals(new AddEntriesResponse(HttpStatus.GATEWAY_TIMEOUT, false, false, List.of()),
                leaderService.addEntries(new AddEntries(List.of("A"))));
        StatusResponse snapshot = leaderService.status();
        assertEquals(List.of(new LogEntry(1, 1, "A")), snapshot.entries());
        assertEquals(0, snapshot.commitIndex());
    }

    @Test
    void waitsForEntireBatchEvenWhenAnEarlierIndexIsCommitted() throws Exception {
        electLeader(raftNode);
        raftNode.getClusterProperties().setCommitTimeout(5000);
        CountDownLatch sent = signalReplicationStart(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var response = executor.submit(() -> service.addEntries(new AddEntries(List.of("A", "B"))));
            assertTrue(sent.await(1, TimeUnit.SECONDS));
            raftNode.setCommitIndex(1);
            applyService.applyCommittedEntries();
            assertThrows(TimeoutException.class, () -> response.get(50, TimeUnit.MILLISECONDS));
            raftNode.setCommitIndex(2);
            applyService.applyCommittedEntries();
            assertEquals(appliedResponse(1, "A", "B"), response.get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void leadershipLossWakesWaitingClientWithoutWaitingForTimeout() throws Exception {
        electLeader(raftNode);
        raftNode.getClusterProperties().setCommitTimeout(5000);
        CountDownLatch sent = signalReplicationStart(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var response = executor.submit(() -> service.addEntries(new AddEntries(List.of("A"))));
            assertTrue(sent.await(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> response.get(50, TimeUnit.MILLISECONDS));
            raftNode.becomeFollower(2);
            assertEquals(new AddEntriesResponse(HttpStatus.CONFLICT, false, false, List.of()), response.get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void eachConcurrentClientWaitsForItsOwnBatch() throws Exception {
        electLeader(raftNode);
        raftNode.getClusterProperties().setCommitTimeout(5000);
        CountDownLatch sent = signalReplicationStart(2);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = executor.submit(() -> service.addEntries(new AddEntries(List.of("A"))));
            // Дожидаемся первого добавления, чтобы индексы A и B были определены.
            verify(replicationService, timeout(1000)).replicateLog();
            var second = executor.submit(() -> service.addEntries(new AddEntries(List.of("B"))));
            assertTrue(sent.await(1, TimeUnit.SECONDS));
            raftNode.setCommitIndex(1);
            applyService.applyCommittedEntries();
            assertEquals(appliedResponse(1, "A"), first.get(1, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> second.get(50, TimeUnit.MILLISECONDS));
            raftNode.setCommitIndex(2);
            applyService.applyCommittedEntries();
            assertEquals(appliedResponse(2, "B"), second.get(1, TimeUnit.SECONDS));
            assertEquals(List.of(new LogEntry(1, 1, "A"), new LogEntry(2, 1, "B")),
                    raftNode.getLogEntries());
        }
    }

    @Test
    void commitAloneDoesNotCompleteRequestUntilStateMachineReturnsResult() throws Exception {
        electLeader(raftNode);
        raftNode.getClusterProperties().setCommitTimeout(5000);
        CountDownLatch sent = signalReplicationStart(1);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var response = executor.submit(() -> service.addEntries(new AddEntries(List.of("A"))));
            assertTrue(sent.await(1, TimeUnit.SECONDS));
            raftNode.setCommitIndex(1);
            assertThrows(TimeoutException.class, () -> response.get(50, TimeUnit.MILLISECONDS));
            assertEquals(0, raftNode.getLastApplied());

            applyService.applyCommittedEntries();
            assertEquals(appliedResponse(1, "A"), response.get(1, TimeUnit.SECONDS));
        }
    }

    @Test
    void applicationFailureDoesNotReturnSuccessForCommittedBatch() {
        electLeader(raftNode);
        NetworkService network = mock(NetworkService.class);
        when(network.sendAppendEntries(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new AppendEntriesResponse(true, 1)));
        ApplyService failingApplication = new ApplyService(raftNode, _ -> {
            throw new IllegalStateException("state machine unavailable");
        });
        RaftService service = new RaftService(raftNode,
                new ReplicationService(raftNode, network, failingApplication), failingApplication);

        assertEquals(new AddEntriesResponse(HttpStatus.GATEWAY_TIMEOUT, true, false, List.of()),
                service.addEntries(new AddEntries(List.of("A"))));
        assertEquals(1, raftNode.getCommitIndex());
        assertEquals(0, raftNode.getLastApplied());
    }

    @Test
    void interruptedWaitReturnsUnavailableAndPreservesInterruptFlag() throws Exception {
        electLeader(raftNode);
        raftNode.getClusterProperties().setCommitTimeout(5000);
        CountDownLatch sent = signalReplicationStart(1);
        AtomicReference<AddEntriesResponse> response = new AtomicReference<>();
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread client = Thread.ofPlatform().start(() -> {
            response.set(service.addEntries(new AddEntries(List.of("A"))));
            interrupted.set(Thread.currentThread().isInterrupted());
        });
        try {
            assertTrue(sent.await(1, TimeUnit.SECONDS));
            client.interrupt();
            client.join(1000);
            assertFalse(client.isAlive());
            assertEquals(new AddEntriesResponse(HttpStatus.SERVICE_UNAVAILABLE, false, false, List.of()), response.get());
            assertTrue(interrupted.get());
        } finally {
            client.interrupt();
            client.join(1000);
        }
    }

    private CountDownLatch signalReplicationStart(int count) {
        CountDownLatch sent = new CountDownLatch(count);
        doAnswer(_ -> {
            assertFalse(Thread.holdsLock(raftNode));
            sent.countDown();
            return null;
        }).when(replicationService).replicateLog();
        return sent;
    }
}
