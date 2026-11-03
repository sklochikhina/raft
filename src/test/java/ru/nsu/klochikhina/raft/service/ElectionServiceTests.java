package ru.nsu.klochikhina.raft.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.RequestVote;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.model.NodeStatus;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static ru.nsu.klochikhina.raft.support.RaftTestFixture.*;

class ElectionServiceTests {

    private RaftNode node;
    private NetworkService network;
    private ElectionService service;

    @BeforeEach
    void setUp() {
        node = node(3);
        network = mock(NetworkService.class);
        service = new ElectionService(node, network);
    }

    @Test
    void recentHeartbeatDoesNotStartElection() {
        node.setLastHeartbeat(System.currentTimeMillis());
        node.setElectionTimeout(Long.MAX_VALUE);

        service.checkElectionTimeout();

        assertEquals(NodeStatus.FOLLOWER, node.getStatus());
        assertEquals(0, node.getCurrentTerm().get());
        verifyNoInteractions(network);
    }

    @Test
    void leaderDoesNotStartElectionEvenWhenTimerHasExpired() {
        electLeader(node);
        expireTimer();

        service.checkElectionTimeout();

        assertEquals(NodeStatus.LEADER, node.getStatus());
        assertEquals(1, node.getCurrentTerm().get());
        verifyNoInteractions(network);
    }

    @Test
    void expiredFollowerStartsElectionWithActualLogMetadataOutsideNodeLock() {
        node.getCurrentTerm().set(2);
        node.getLogEntries().addAll(List.of(new LogEntry(1, "A"), new LogEntry(2, "B")));
        expireTimer();
        doAnswer(_ -> {
            assertFalse(Thread.holdsLock(node));
            return CompletableFuture.completedFuture(null);
        }).when(network).sendRequestVote(any());

        service.checkElectionTimeout();

        assertEquals(NodeStatus.CANDIDATE, node.getStatus());
        assertEquals(3, node.getCurrentTerm().get());
        assertEquals(1, node.getVotedFor());
        assertTrue(node.getLastHeartbeat() > 0);
        verify(network).sendRequestVote(new RequestVote(3, 1, 2, 2));
    }

    @Test
    void candidateRetriesElectionWithNewTermAfterTimeout() {
        node.becomeCandidate();
        expireTimer();

        service.checkElectionTimeout();

        assertEquals(NodeStatus.CANDIDATE, node.getStatus());
        assertEquals(2, node.getCurrentTerm().get());
        verify(network).sendRequestVote(new RequestVote(2, 1, 0, 0));
    }

    @Test
    void singleNodeElectsItselfWithoutHttpRequests() {
        RaftNode singleNode = node(1);
        singleNode.setLastHeartbeat(0);
        singleNode.setElectionTimeout(0);
        NetworkService localNetwork = new NetworkService(null, singleNode, singleNode.getClusterProperties());

        new ElectionService(singleNode, localNetwork).checkElectionTimeout();

        assertEquals(NodeStatus.LEADER, singleNode.getStatus());
        assertEquals(1, singleNode.getCurrentTerm().get());
    }

    private void expireTimer() {
        node.setLastHeartbeat(0);
        node.setElectionTimeout(0);
    }
}
