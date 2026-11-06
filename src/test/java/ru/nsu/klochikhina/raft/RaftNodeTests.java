package ru.nsu.klochikhina.raft;

import org.junit.jupiter.api.Test;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.model.NodeStatus;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static ru.nsu.klochikhina.raft.support.RaftTestFixture.*;

class RaftNodeTests {

    @Test
    void startsAsFollowerWithEmptyUncommittedLogAndRandomizedTimeout() {
        RaftNode node = node(3);

        assertEquals(NodeStatus.FOLLOWER, node.getStatus());
        assertEquals(0, node.getCurrentTerm().get());
        assertNull(node.getVotedFor());
        assertTrue(node.getLogEntries().isEmpty());
        assertEquals(0, node.getCommitIndex());
        assertTrue(node.getElectionTimeout() >= 250 && node.getElectionTimeout() < 451);
    }

    @Test
    void newElectionIncrementsTermAndVotesForSelfWithoutLosingLog() {
        RaftNode node = node(3);
        node.getLogEntries().add(new LogEntry(1, "A"));
        node.getCurrentTerm().set(1);
        node.setCommitIndex(1);

        node.becomeCandidate();

        assertEquals(NodeStatus.CANDIDATE, node.getStatus());
        assertEquals(2, node.getCurrentTerm().get());
        assertEquals(1, node.getVotedFor());
        assertEquals(List.of(new LogEntry(1, "A")), node.getLogEntries());
        assertEquals(1, node.getCommitIndex());
    }

    @Test
    void onlyCandidateCanBecomeLeader() {
        RaftNode node = node(3);
        node.becomeLeader();

        assertEquals(NodeStatus.FOLLOWER, node.getStatus());
        assertTrue(node.getNextIndex().isEmpty());
    }

    @Test
    void eachLeadershipInitializesFollowerProgressFromCurrentLog() {
        RaftNode node = node(3);
        node.getLogEntries().addAll(List.of(new LogEntry(1, "A"), new LogEntry(1, "B")));
        electLeader(node);
        assertEquals(Map.of(2, 3, 3, 3), node.getNextIndex());
        assertEquals(Map.of(2, 0, 3, 0), node.getMatchIndex());

        node.getNextIndex().put(2, 1);
        node.getMatchIndex().put(2, 2);
        node.becomeFollower(2);
        node.getLogEntries().add(new LogEntry(2, "C"));
        electLeader(node);

        assertEquals(Map.of(2, 4, 3, 4), node.getNextIndex());
        assertEquals(Map.of(2, 0, 3, 0), node.getMatchIndex());
    }

    @Test
    void olderTermCannotDemoteLeaderOrEraseVote() {
        RaftNode node = node(3);
        electLeader(node);

        node.becomeFollower(0);

        assertEquals(NodeStatus.LEADER, node.getStatus());
        assertEquals(1, node.getCurrentTerm().get());
        assertEquals(1, node.getVotedFor());
    }

    @Test
    void sameTermDemotionPreservesVoteButNewTermClearsIt() {
        RaftNode node = node(3);
        electLeader(node);
        node.becomeFollower(1);
        assertEquals(1, node.getVotedFor());

        node.becomeFollower(2);

        assertEquals(NodeStatus.FOLLOWER, node.getStatus());
        assertEquals(2, node.getCurrentTerm().get());
        assertNull(node.getVotedFor());
    }

    @Test
    void resetTimerRefreshesLastContactAndKeepsTimeoutInRange() {
        RaftNode node = node(3);
        node.setLastHeartbeat(0);

        node.resetElectionTimer();

        assertTrue(node.getLastHeartbeat() > 0);
        assertTrue(node.getElectionTimeout() >= 250 && node.getElectionTimeout() < 451);
    }
}
