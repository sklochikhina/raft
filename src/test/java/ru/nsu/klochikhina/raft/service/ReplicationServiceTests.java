package ru.nsu.klochikhina.raft.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.AppendEntries;
import ru.nsu.klochikhina.raft.dto.AppendEntriesResponse;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.model.NodeConfig;
import ru.nsu.klochikhina.raft.model.NodeInfo;
import ru.nsu.klochikhina.raft.model.NodeStatus;
import ru.nsu.klochikhina.raft.property.ClusterProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static ru.nsu.klochikhina.raft.support.RaftTestFixture.node;
import static ru.nsu.klochikhina.raft.support.RaftTestFixture.application;

class ReplicationServiceTests {

    private final NodeInfo firstFollower = new NodeInfo(2, 8082, "localhost");
    private final NodeInfo secondFollower = new NodeInfo(3, 8083, "localhost");

    private RaftNode raftNode;
    private RecordingNetworkService networkService;
    private ReplicationService replicationService;

    @BeforeEach
    void setUp() {
        setUpLeader(3);
    }

    private void setUpLeader(int clusterSize) {
        NodeConfig config = new NodeConfig() {
            @Override
            public int getNodeId() {
                return 1;
            }
        };
        ClusterProperties cluster = new ClusterProperties();
        List<NodeInfo> nodes = new ArrayList<>();
        for (int id = 1; id <= clusterSize; id++) {
            nodes.add(new NodeInfo(id, 8080 + id, "localhost"));
        }
        cluster.setNodes(nodes);

        raftNode = new RaftNode(config, cluster);
        raftNode.init();
        raftNode.getLogEntries().addAll(List.of(
                new LogEntry(1, "A"),
                new LogEntry(1, "B"),
                new LogEntry(2, "C"),
                new LogEntry(3, "D")
        ));
        raftNode.becomeCandidate();
        raftNode.getCurrentTerm().set(3);
        raftNode.becomeLeader();
        raftNode.setCommitIndex(2);

        networkService = new RecordingNetworkService(raftNode, cluster);
        replicationService = new ReplicationService(raftNode, networkService, application(raftNode));
    }

    @Test
    void sendsEachFollowerItsOwnSuffixAndPreviousEntry() {
        raftNode.getNextIndex().put(2, 2);
        raftNode.getNextIndex().put(3, 4);

        replicationService.replicateLog();

        assertEquals(List.of(
                new SentRequest(firstFollower, new AppendEntries(
                        3, 1, 1, 1,
                        List.of(new LogEntry(1, "B"), new LogEntry(2, "C"), new LogEntry(3, "D")),
                        2
                )),
                new SentRequest(secondFollower, new AppendEntries(
                        3, 1, 3, 2, List.of(new LogEntry(3, "D")), 2
                ))
        ), networkService.requests);
    }

    @Test
    void heartbeatUsesLastEntryWhenFollowersAreCaughtUp() {
        new HeartbeatService(replicationService).start();

        assertEquals(List.of(
                new SentRequest(firstFollower, new AppendEntries(3, 1, 4, 3, List.of(), 2)),
                new SentRequest(secondFollower, new AppendEntries(3, 1, 4, 3, List.of(), 2))
        ), networkService.requests);
    }

    @Test
    void emptyLogUsesZeroPreviousIndexAndTerm() {
        raftNode.getLogEntries().clear();
        raftNode.setCommitIndex(0);
        raftNode.getNextIndex().replaceAll((_, _) -> 1);

        replicationService.replicateLog();

        assertEquals(List.of(
                new SentRequest(firstFollower, new AppendEntries(3, 1, 0, 0, List.of(), 0)),
                new SentRequest(secondFollower, new AppendEntries(3, 1, 0, 0, List.of(), 0))
        ), networkService.requests);
    }

    @Test
    void requestsKeepTheirEntriesWhenLocalLogChanges() {
        raftNode.getNextIndex().put(2, 1);
        replicationService.replicateLog();
        List<LogEntry> expectedEntries = List.copyOf(raftNode.getLogEntries());

        raftNode.getLogEntries().clear();

        assertEquals(expectedEntries, networkService.requests.getFirst().request().entries());
    }

    @Test
    void followerAndCandidateDoNotSendAppendEntries() {
        raftNode.setStatus(NodeStatus.FOLLOWER);
        replicationService.replicateLog();
        raftNode.setStatus(NodeStatus.CANDIDATE);
        replicationService.replicateLog();

        assertTrue(networkService.requests.isEmpty());
    }

    @Test
    void successConfirmsOnlyEntriesFromTheSentRequest() {
        raftNode.getNextIndex().put(2, 2);
        replicationService.replicateLog();
        raftNode.getLogEntries().add(new LogEntry(3, "E"));

        networkService.responses.getFirst().complete(new AppendEntriesResponse(true, 3));

        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(0, raftNode.getMatchIndex().get(3));
        assertEquals(4, raftNode.getCommitIndex());

        replicationService.replicateLog();
        assertEquals(new SentRequest(firstFollower,
                new AppendEntries(3, 1, 4, 3, List.of(new LogEntry(3, "E")), 4)),
                networkService.requests.get(2));
    }

    @Test
    void successfulHeartbeatConfirmsThePreviousEntry() {
        replicationService.replicateLog();

        networkService.responses.getFirst().complete(new AppendEntriesResponse(true, 3));

        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(4, raftNode.getCommitIndex());
    }

    @Test
    void backtracksUntilFollowerCanReplaceItsConflictingTail() {
        NodeConfig followerConfig = new NodeConfig() {
            @Override
            public int getNodeId() {
                return 2;
            }
        };
        RaftNode follower = new RaftNode(followerConfig, raftNode.getClusterProperties());
        follower.init();
        follower.getLogEntries().addAll(List.of(
                new LogEntry(1, "A"), new LogEntry(1, "B"),
                new LogEntry(1, "X"), new LogEntry(1, "Y")
        ));
        RaftService followerService = new RaftService(follower, mock(ReplicationService.class), application(follower));

        replicationService.replicateLog();
        completeFromFollower(0, followerService);
        assertEquals(1, raftNode.getNextIndex().get(2));
        assertEquals(0, raftNode.getMatchIndex().get(2));

        replicationService.replicateLog();
        completeFromFollower(2, followerService);

        assertEquals(raftNode.getLogEntries(), follower.getLogEntries());
        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(4, raftNode.getCommitIndex());
        assertEquals(2, follower.getCommitIndex());

        replicationService.replicateLog();
        assertEquals(4, networkService.requests.get(3).request().leaderCommit());
        assertTrue(networkService.requests.get(3).request().entries().isEmpty());
        completeFromFollower(3, followerService);

        assertEquals(4, follower.getCommitIndex());
        assertEquals(4, raftNode.getLastApplied());
        assertEquals(4, follower.getLastApplied());
    }

    @Test
    void rejectionDoesNotMoveNextIndexBelowOne() {
        raftNode.getNextIndex().put(2, 1);
        replicationService.replicateLog();

        networkService.responses.getFirst().complete(new AppendEntriesResponse(false, 3));

        assertEquals(1, raftNode.getNextIndex().get(2));
        assertEquals(0, raftNode.getMatchIndex().get(2));
    }

    @Test
    void doesNotOverlapRequestsToTheSameFollower() {
        replicationService.replicateLog();
        replicationService.replicateLog();
        assertEquals(2, networkService.requests.size());

        networkService.responses.getFirst().complete(new AppendEntriesResponse(true, 3));
        replicationService.replicateLog();

        assertEquals(3, networkService.requests.size());
        assertEquals(firstFollower, networkService.requests.get(2).follower());
    }

    @Test
    void transportFailureLeavesIndicesUnchangedAndAllowsRetry() {
        replicationService.replicateLog();

        networkService.responses.getFirst().completeExceptionally(new RuntimeException("timeout"));

        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(0, raftNode.getMatchIndex().get(2));
        replicationService.replicateLog();
        assertEquals(3, networkService.requests.size());
    }

    @Test
    void missingResponseLeavesIndicesUnchangedAndAllowsRetry() {
        replicationService.replicateLog();

        networkService.responses.getFirst().complete(null);

        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(0, raftNode.getMatchIndex().get(2));
        replicationService.replicateLog();
        assertEquals(3, networkService.requests.size());
    }

    @Test
    void oldTermResponseCannotUpdateIndicesOrReleaseNewRequest() {
        replicationService.replicateLog();
        raftNode.becomeCandidate();
        raftNode.becomeLeader();
        replicationService.replicateLog();

        networkService.responses.getFirst().complete(new AppendEntriesResponse(true, 3));
        replicationService.replicateLog();

        assertEquals(4, networkService.requests.size());
        assertEquals(0, raftNode.getMatchIndex().get(2));
        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(2, raftNode.getCommitIndex());

        networkService.responses.get(2).complete(new AppendEntriesResponse(true, 4));
        assertEquals(4, raftNode.getMatchIndex().get(2));
    }

    @Test
    void responseAfterSteppingDownCannotUpdateIndices() {
        replicationService.replicateLog();
        raftNode.becomeFollower(3);

        networkService.responses.getFirst().complete(new AppendEntriesResponse(true, 3));

        assertEquals(NodeStatus.FOLLOWER, raftNode.getStatus());
        assertEquals(0, raftNode.getMatchIndex().get(2));
        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(2, raftNode.getCommitIndex());
    }

    @Test
    void higherTermFromOldRequestStillMakesLeaderStepDown() {
        replicationService.replicateLog();
        raftNode.becomeCandidate();
        raftNode.becomeLeader();
        replicationService.replicateLog();

        networkService.responses.getFirst().complete(new AppendEntriesResponse(false, 5));

        assertEquals(NodeStatus.FOLLOWER, raftNode.getStatus());
        assertEquals(5, raftNode.getCurrentTerm().get());
        assertEquals(0, raftNode.getMatchIndex().get(2));
        networkService.responses.get(2).complete(new AppendEntriesResponse(true, 4));
        assertEquals(0, raftNode.getMatchIndex().get(2));
        assertEquals(2, raftNode.getCommitIndex());
    }

    @Test
    void eachIndexNeedsItsOwnMajorityInFiveNodeCluster() {
        setUpLeader(5);
        replicationService.replicateLog();
        raftNode.getLogEntries().add(new LogEntry(3, "E"));

        networkService.responses.get(0).complete(new AppendEntriesResponse(true, 3));
        assertEquals(2, raftNode.getCommitIndex());

        networkService.responses.get(1).complete(new AppendEntriesResponse(true, 3));
        assertEquals(4, raftNode.getCommitIndex());

        replicationService.replicateLog();
        networkService.responses.get(4).complete(new AppendEntriesResponse(true, 3));
        assertEquals(4, raftNode.getCommitIndex());

        networkService.responses.get(5).complete(new AppendEntriesResponse(true, 3));
        assertEquals(5, raftNode.getCommitIndex());
    }

    @Test
    void exactlyHalfOfEvenSizedClusterIsNotAMajority() {
        setUpLeader(4);
        replicationService.replicateLog();

        networkService.responses.get(0).complete(new AppendEntriesResponse(true, 3));
        assertEquals(2, raftNode.getCommitIndex());

        networkService.responses.get(1).complete(new AppendEntriesResponse(true, 3));
        assertEquals(4, raftNode.getCommitIndex());
    }

    @Test
    void oldTermsCommitOnlyWhenCurrentTermEntryReachesMajority() {
        raftNode.becomeCandidate();
        raftNode.becomeLeader();
        replicationService.replicateLog();

        networkService.responses.get(0).complete(new AppendEntriesResponse(true, 4));
        networkService.responses.get(1).complete(new AppendEntriesResponse(true, 4));
        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(4, raftNode.getMatchIndex().get(3));
        assertEquals(2, raftNode.getCommitIndex());

        raftNode.getLogEntries().add(new LogEntry(4, "E"));
        replicationService.replicateLog();
        assertEquals(2, raftNode.getCommitIndex());

        networkService.responses.get(2).complete(new AppendEntriesResponse(true, 4));
        assertEquals(5, raftNode.getCommitIndex());
        assertEquals(5, raftNode.getLastApplied());
    }

    @Test
    void newLeadershipDoesNotReduceCommittedPrefix() {
        replicationService.replicateLog();
        networkService.responses.getFirst().complete(new AppendEntriesResponse(true, 3));
        assertEquals(4, raftNode.getCommitIndex());

        raftNode.becomeCandidate();
        raftNode.becomeLeader();
        replicationService.replicateLog();

        assertEquals(0, raftNode.getMatchIndex().get(2));
        assertEquals(0, raftNode.getMatchIndex().get(3));
        assertEquals(4, raftNode.getCommitIndex());
    }

    @Test
    void singleNodeLeaderCommitsWithoutRemoteResponses() {
        setUpLeader(1);

        replicationService.replicateLog();

        assertEquals(4, raftNode.getCommitIndex());
        assertEquals(4, raftNode.getLastApplied());
        assertTrue(networkService.requests.isEmpty());
    }

    @Test
    void restoresPreviouslyConfirmedFollowerAfterItLosesItsEntireLogWithoutNewElection() {
        RaftNode follower = node(2, 3);
        follower.getLogEntries().addAll(raftNode.getLogEntries());
        RaftService followerService = new RaftService(follower, mock(ReplicationService.class), application(follower));
        replicationService.replicateLog();
        completeFromFollower(0, followerService);
        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(4, raftNode.getCommitIndex());

        RaftNode restartedFollower = node(2, 3);
        RaftService restartedService = new RaftService(restartedFollower, mock(ReplicationService.class), application(restartedFollower));
        replicationService.replicateLog();
        completeFromFollower(2, restartedService);

        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(4, raftNode.getCommitIndex());

        replicationService.replicateLog();
        AppendEntries recovery = networkService.requests.get(3).request();
        assertEquals(0, recovery.prevLogIndex());
        assertEquals(raftNode.getLogEntries(), recovery.entries());
        completeFromFollower(3, restartedService);

        assertEquals(raftNode.getLogEntries(), restartedFollower.getLogEntries());
        assertEquals(4, restartedFollower.getCommitIndex());
        assertEquals(4, restartedFollower.getLastApplied());
        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(NodeStatus.LEADER, raftNode.getStatus());
        assertEquals(3, raftNode.getCurrentTerm().get());
    }

    @Test
    void followerRejectingPreviouslyConfirmedPrefixDoesNotCountTowardsNewCommit() {
        setUpLeader(5);
        replicationService.replicateLog();
        networkService.responses.getFirst().complete(new AppendEntriesResponse(true, 3));
        assertEquals(2, raftNode.getCommitIndex());

        replicationService.replicateLog();
        networkService.responses.get(4).complete(new AppendEntriesResponse(false, 3));
        networkService.responses.get(1).complete(new AppendEntriesResponse(true, 3));

        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(4, raftNode.getMatchIndex().get(3));
        assertEquals(2, raftNode.getCommitIndex());

        networkService.responses.get(2).complete(new AppendEntriesResponse(true, 3));
        assertEquals(4, raftNode.getCommitIndex());
    }

    @Test
    void transportTimeoutDoesNotErasePreviouslyConfirmedProgress() {
        replicationService.replicateLog();
        networkService.responses.get(0).complete(new AppendEntriesResponse(true, 3));
        replicationService.replicateLog();

        networkService.responses.get(2).completeExceptionally(new RuntimeException("timeout"));

        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(4, raftNode.getCommitIndex());
    }

    @Test
    void oldTermRejectionCannotEraseNewTermsConfirmedProgress() {
        replicationService.replicateLog();
        raftNode.becomeCandidate();
        raftNode.becomeLeader();
        replicationService.replicateLog();
        networkService.responses.get(2).complete(new AppendEntriesResponse(true, 4));

        networkService.responses.get(0).complete(new AppendEntriesResponse(false, 3));

        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(5, raftNode.getNextIndex().get(2));
        assertEquals(4, raftNode.getCurrentTerm().get());
    }

    @Test
    void ordinaryMismatchKeepsMatchIndexAndRecoveryStartsOnlyAtConfirmedBoundary() {
        raftNode.getMatchIndex().put(2, 2);
        replicationService.replicateLog();
        networkService.responses.getFirst().complete(new AppendEntriesResponse(false, 3, 1));

        assertEquals(2, raftNode.getMatchIndex().get(2));
        assertEquals(3, raftNode.getNextIndex().get(2));
        replicationService.replicateLog();
        assertEquals(2, networkService.requests.get(2).request().prevLogIndex());

        networkService.responses.get(2).complete(new AppendEntriesResponse(false, 3, 1));
        assertEquals(2, raftNode.getMatchIndex().get(2));
        assertEquals(3, raftNode.getNextIndex().get(2));
        replicationService.replicateLog();
        assertEquals(0, networkService.requests.get(3).request().prevLogIndex());
        assertEquals(raftNode.getLogEntries(), networkService.requests.get(3).request().entries());
    }

    @Test
    void timeoutDuringRecoveryRetriesFullLogAndSuccessfulRecoveryReturnsToNormalReplication() {
        replicationService.replicateLog();
        networkService.responses.get(0).complete(new AppendEntriesResponse(true, 3));
        replicationService.replicateLog();
        networkService.responses.get(2).complete(new AppendEntriesResponse(false, 3));
        replicationService.replicateLog();
        assertEquals(0, networkService.requests.get(3).request().prevLogIndex());

        networkService.responses.get(3).completeExceptionally(new RuntimeException("timeout"));
        replicationService.replicateLog();
        assertEquals(0, networkService.requests.get(4).request().prevLogIndex());
        assertEquals(4, raftNode.getMatchIndex().get(2));
        assertEquals(5, raftNode.getNextIndex().get(2));

        networkService.responses.get(4).complete(new AppendEntriesResponse(true, 3));
        replicationService.replicateLog();
        assertEquals(4, networkService.requests.get(5).request().prevLogIndex());
        assertTrue(networkService.requests.get(5).request().entries().isEmpty());
    }

    @Test
    void newLeadershipIgnoresOldRecoveryAndOldCompletionCannotReleaseNewRequest() {
        replicationService.replicateLog();
        networkService.responses.get(0).complete(new AppendEntriesResponse(true, 3));
        replicationService.replicateLog();
        networkService.responses.get(2).complete(new AppendEntriesResponse(false, 3));
        replicationService.replicateLog();

        raftNode.becomeCandidate();
        raftNode.becomeLeader();
        replicationService.replicateLog();
        assertEquals(4, networkService.requests.get(4).request().prevLogIndex());
        networkService.responses.get(3).complete(new AppendEntriesResponse(true, 3));
        replicationService.replicateLog();
        assertEquals(6, networkService.requests.size());

        networkService.responses.get(4).complete(new AppendEntriesResponse(true, 4));
        raftNode.getLogEntries().add(new LogEntry(4, "E"));
        replicationService.replicateLog();
        networkService.responses.get(6).complete(new AppendEntriesResponse(true, 4));
        assertEquals(5, raftNode.getCommitIndex());
        assertEquals(5, raftNode.getMatchIndex().get(2));
    }

    private void completeFromFollower(int requestIndex, RaftService followerService) {
        AppendEntries request = networkService.requests.get(requestIndex).request();
        networkService.responses.get(requestIndex).complete(followerService.appendEntries(request));
    }

    private record SentRequest(NodeInfo follower, AppendEntries request) {
    }

    private static class RecordingNetworkService extends NetworkService {
        private final RaftNode raftNode;
        private final List<SentRequest> requests = new ArrayList<>();
        private final List<CompletableFuture<AppendEntriesResponse>> responses = new ArrayList<>();

        RecordingNetworkService(RaftNode raftNode, ClusterProperties cluster) {
            super(null, raftNode, cluster);
            this.raftNode = raftNode;
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> sendAppendEntries(NodeInfo follower, AppendEntries request) {
            assertFalse(Thread.holdsLock(raftNode), "HTTP must run outside the node lock");
            requests.add(new SentRequest(follower, request));
            CompletableFuture<AppendEntriesResponse> response = new CompletableFuture<>();
            responses.add(response);
            return response;
        }
    }
}
