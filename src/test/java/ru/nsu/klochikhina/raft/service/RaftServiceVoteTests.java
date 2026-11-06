package ru.nsu.klochikhina.raft.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.AppendEntries;
import ru.nsu.klochikhina.raft.dto.RequestVote;
import ru.nsu.klochikhina.raft.dto.RequestVoteResponse;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.model.NodeConfig;
import ru.nsu.klochikhina.raft.model.NodeInfo;
import ru.nsu.klochikhina.raft.model.NodeStatus;
import ru.nsu.klochikhina.raft.property.ClusterProperties;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class RaftServiceVoteTests {

    private RaftNode raftNode;
    private RaftService raftService;

    @BeforeEach
    void setUp() {
        NodeConfig config = new NodeConfig() {
            @Override
            public int getNodeId() {
                return 1;
            }
        };
        ClusterProperties cluster = new ClusterProperties();
        cluster.setNodes(List.of(
                new NodeInfo(1, 8081, "localhost"),
                new NodeInfo(2, 8082, "localhost"),
                new NodeInfo(3, 8083, "localhost")
        ));
        raftNode = new RaftNode(config, cluster);
        raftNode.init();
        raftNode.getCurrentTerm().set(3);
        raftNode.getLogEntries().addAll(List.of(
                new LogEntry(1, "A"), new LogEntry(1, "B"), new LogEntry(2, "C")
        ));
        raftNode.setLastHeartbeat(100);
        raftService = new RaftService(raftNode, mock(ReplicationService.class));
    }

    @ParameterizedTest
    @CsvSource({
            "1, 100, false",
            "2, 2, false",
            "2, 3, true",
            "2, 4, true",
            "3, 1, true",
            "0, 0, false"
    })
    void comparesLastEntryTermBeforeLogLength(long lastLogTerm, int lastLogIndex, boolean expected) {
        RequestVoteResponse response = raftService.vote(new RequestVote(3, 2, lastLogTerm, lastLogIndex));

        assertEquals(new RequestVoteResponse(expected, 3), response);
        if (expected) {
            assertEquals(2, raftNode.getVotedFor());
            assertTrue(raftNode.getLastHeartbeat() > 100);
        } else {
            assertNull(raftNode.getVotedFor());
            assertEquals(100, raftNode.getLastHeartbeat());
        }
    }

    @Test
    void emptyLogsCanGrantVote() {
        raftNode.getLogEntries().clear();

        assertEquals(new RequestVoteResponse(true, 3),
                raftService.vote(new RequestVote(3, 2, 0, 0)));
    }

    @Test
    void emptyVoterCanGrantVoteToNonEmptyCandidate() {
        raftNode.getLogEntries().clear();

        assertEquals(new RequestVoteResponse(true, 3),
                raftService.vote(new RequestVote(3, 2, 2, 1)));
    }

    @Test
    void rejectedLogDoesNotConsumeVote() {
        assertFalse(raftService.vote(new RequestVote(3, 2, 1, 100)).voteGranted());

        assertTrue(raftService.vote(new RequestVote(3, 3, 2, 3)).voteGranted());
        assertEquals(3, raftNode.getVotedFor());
    }

    @Test
    void repeatedVoteForSameCandidateIsAllowedButAnotherCandidateIsRejected() {
        RequestVote request = new RequestVote(3, 2, 2, 3);
        assertTrue(raftService.vote(request).voteGranted());
        assertTrue(raftService.vote(request).voteGranted());
        raftNode.setLastHeartbeat(100);

        assertFalse(raftService.vote(new RequestVote(3, 3, 3, 1)).voteGranted());
        assertEquals(2, raftNode.getVotedFor());
        assertEquals(100, raftNode.getLastHeartbeat());
    }

    @Test
    void olderElectionTermIsRejectedEvenIfCandidateLogIsUpToDate() {
        raftNode.setVotedFor(3);

        assertEquals(new RequestVoteResponse(false, 3),
                raftService.vote(new RequestVote(2, 2, 2, 100)));
        assertEquals(3, raftNode.getCurrentTerm().get());
        assertEquals(3, raftNode.getVotedFor());
        assertEquals(100, raftNode.getLastHeartbeat());
    }

    @Test
    void higherElectionTermAllowsNewVoteWhenCandidateLogIsUpToDate() {
        raftNode.setVotedFor(2);

        assertEquals(new RequestVoteResponse(true, 4),
                raftService.vote(new RequestVote(4, 3, 2, 3)));
        assertEquals(4, raftNode.getCurrentTerm().get());
        assertEquals(3, raftNode.getVotedFor());
    }

    @Test
    void higherElectionTermStepsDownEvenWhenCandidateLogIsOutdated() {
        raftNode.becomeCandidate();
        raftNode.becomeLeader();

        assertEquals(new RequestVoteResponse(false, 5),
                raftService.vote(new RequestVote(5, 2, 1, 100)));
        assertEquals(5, raftNode.getCurrentTerm().get());
        assertEquals(NodeStatus.FOLLOWER, raftNode.getStatus());
        assertNull(raftNode.getVotedFor());
        assertEquals(100, raftNode.getLastHeartbeat());
    }

    @Test
    void sameTermHeartbeatPreservesVoteAndPreventsVotingTwice() {
        raftNode.becomeCandidate();

        assertTrue(raftService.appendEntries(new AppendEntries(4, 2, 3, 2, List.of(), 0)).success());

        assertEquals(NodeStatus.FOLLOWER, raftNode.getStatus());
        assertEquals(1, raftNode.getVotedFor());
        assertFalse(raftService.vote(new RequestVote(4, 3, 2, 3)).voteGranted());
        assertEquals(1, raftNode.getVotedFor());
    }
}
