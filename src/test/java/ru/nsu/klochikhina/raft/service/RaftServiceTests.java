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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static ru.nsu.klochikhina.raft.support.RaftTestFixture.*;

class RaftServiceTests {

    private RaftNode raftNode;
    private ReplicationService replicationService;
    private RaftService service;

    @BeforeEach
    void setUp() {
        raftNode = node(3);
        replicationService = mock(ReplicationService.class);
        service = new RaftService(raftNode, replicationService);
    }

    @Test
    void leaderAppendsWholeBatchWithItsCurrentTermAndStartsReplicationOutsideLock() {
        raftNode.getLogEntries().add(new LogEntry(1, "old"));
        raftNode.getCurrentTerm().set(1);
        raftNode.setCommitIndex(1);
        electLeader(raftNode);
        doAnswer(_ -> {
            assertFalse(Thread.holdsLock(raftNode));
            assertEquals(3, raftNode.getLastLogIndex());
            return null;
        }).when(replicationService).replicateLog();

        AddEntriesResponse response = service.addEntries(new AddEntries(List.of("first", "second")));

        assertEquals(new AddEntriesResponse(HttpStatus.ACCEPTED, false), response);
        assertEquals(List.of(new LogEntry(1, "old"), new LogEntry(2, "first"), new LogEntry(2, "second")),
                raftNode.getLogEntries());
        assertEquals(1, raftNode.getCommitIndex());
        verify(replicationService).replicateLog();
    }

    @ParameterizedTest
    @EnumSource(value = NodeStatus.class, names = {"FOLLOWER", "CANDIDATE"})
    void nonLeaderRejectsWritesWithoutChangingLog(NodeStatus status) {
        raftNode.setStatus(status);

        assertEquals(new AddEntriesResponse(HttpStatus.CONFLICT, false),
                service.addEntries(new AddEntries(List.of("command"))));
        assertTrue(raftNode.getLogEntries().isEmpty());
        verifyNoInteractions(replicationService);
    }

    @ParameterizedTest
    @MethodSource("invalidBatches")
    void invalidBatchIsRejectedWithoutPartialAppend(AddEntries request) {
        electLeader(raftNode);
        raftNode.getLogEntries().add(new LogEntry(1, "existing"));

        assertEquals(new AddEntriesResponse(HttpStatus.BAD_REQUEST, false), service.addEntries(request));
        assertEquals(List.of(new LogEntry(1, "existing")), raftNode.getLogEntries());
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

        service.addEntries(new AddEntries(commands));

        assertEquals(commands, service.status().entries().stream().map(LogEntry::command).toList());
    }

    @Test
    void singleNodeClusterCanCommitBeforeReturning() {
        RaftNode singleNode = node(1);
        electLeader(singleNode);
        NetworkService network = mock(NetworkService.class);
        RaftService singleNodeService = new RaftService(singleNode,
                new ReplicationService(singleNode, network));

        assertEquals(new AddEntriesResponse(HttpStatus.OK, true),
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
            raftNode.getLogEntries().add(new LogEntry(2, "replacement"));
            raftNode.setCommitIndex(1);
            return null;
        }).when(replicationService).replicateLog();

        assertEquals(new AddEntriesResponse(HttpStatus.ACCEPTED, false),
                service.addEntries(new AddEntries(List.of("original"))));
    }

    @Test
    void statusReturnsImmutableSnapshotWithCommitIndex() {
        raftNode.getLogEntries().addAll(List.of(new LogEntry(1, "A"), new LogEntry(2, "B")));
        raftNode.getCurrentTerm().set(2);
        raftNode.setCommitIndex(1);

        StatusResponse snapshot = service.status();
        raftNode.getLogEntries().clear();
        raftNode.setCommitIndex(0);
        raftNode.becomeCandidate();

        assertEquals(List.of(new LogEntry(1, "A"), new LogEntry(2, "B")), snapshot.entries());
        assertEquals(1, snapshot.commitIndex());
        assertEquals(2, snapshot.currentTerm());
        assertEquals(NodeStatus.FOLLOWER, snapshot.status());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.entries().add(new LogEntry(3, "C")));
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
                List.of(), 0), service.status());
    }

    @Test
    void statusHasFallbackWhenSelfIsMissingFromClusterConfig() {
        raftNode.getClusterProperties().setNodes(List.of(new NodeInfo(2, 8082, "localhost")));

        assertEquals(new NodeInfo(1, 8081, "unknown"), service.status().nodeInfo());
    }

    @Test
    void acceptedBatchReachesFollowerAndCommitBoundaryIsPropagatedAfterMajority() {
        electLeader(raftNode);
        NetworkService network = mock(NetworkService.class);
        ReplicationService replication = new ReplicationService(raftNode, network);
        RaftService leaderService = new RaftService(raftNode, replication);
        RaftNode follower = node(2, 3);
        RaftService followerService = new RaftService(follower, mock(ReplicationService.class));
        NodeInfo peer = raftNode.getClusterProperties().getNodes().get(1);
        CompletableFuture<AppendEntriesResponse> appendResponse = new CompletableFuture<>();
        CompletableFuture<AppendEntriesResponse> heartbeatResponse = new CompletableFuture<>();
        when(network.sendAppendEntries(eq(peer), any())).thenReturn(appendResponse).thenReturn(heartbeatResponse);
        when(network.sendAppendEntries(eq(raftNode.getClusterProperties().getNodes().get(2)), any()))
                .thenReturn(new CompletableFuture<>());

        assertEquals(new AddEntriesResponse(HttpStatus.ACCEPTED, false),
                leaderService.addEntries(new AddEntries(List.of("A", "B"))));
        assertEquals(0, leaderService.status().commitIndex());

        ArgumentCaptor<AppendEntries> requests = ArgumentCaptor.forClass(AppendEntries.class);
        verify(network).sendAppendEntries(eq(peer), requests.capture());
        appendResponse.complete(followerService.appendEntries(requests.getValue()));

        assertEquals(2, leaderService.status().commitIndex());
        assertEquals(0, followerService.status().commitIndex());
        replication.replicateLog();
        verify(network, times(2)).sendAppendEntries(eq(peer), requests.capture());
        heartbeatResponse.complete(followerService.appendEntries(requests.getValue()));

        StatusResponse leaderStatus = leaderService.status();
        StatusResponse followerStatus = followerService.status();
        assertEquals(leaderStatus.entries(), followerStatus.entries());
        assertEquals(leaderStatus.commitIndex(), followerStatus.commitIndex());
        assertEquals(List.of(new LogEntry(1, "A"), new LogEntry(1, "B")), followerStatus.entries());
    }

    @Test
    void batchRemainsUncommittedWhenNoFollowerConfirmsIt() {
        electLeader(raftNode);
        NetworkService network = mock(NetworkService.class);
        when(network.sendAppendEntries(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(new AppendEntriesResponse(false, 1)));
        RaftService leaderService = new RaftService(raftNode,
                new ReplicationService(raftNode, network));

        assertEquals(new AddEntriesResponse(HttpStatus.ACCEPTED, false),
                leaderService.addEntries(new AddEntries(List.of("A"))));
        StatusResponse snapshot = leaderService.status();
        assertEquals(List.of(new LogEntry(1, "A")), snapshot.entries());
        assertEquals(0, snapshot.commitIndex());
    }
}
