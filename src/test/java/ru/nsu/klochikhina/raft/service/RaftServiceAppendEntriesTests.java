package ru.nsu.klochikhina.raft.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.AppendEntries;
import ru.nsu.klochikhina.raft.dto.AppendEntriesResponse;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.model.NodeStatus;
import ru.nsu.klochikhina.raft.support.LogCapture;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static ru.nsu.klochikhina.raft.support.RaftTestFixture.*;

class RaftServiceAppendEntriesTests {

    private RaftNode node;
    private RaftService service;
    private List<LogEntry> originalLog;

    @BeforeEach
    void setUp() {
        node = node(3);
        node.getCurrentTerm().set(3);
        originalLog = List.of(new LogEntry(1, "A"), new LogEntry(1, "B"),
                new LogEntry(2, "X"), new LogEntry(2, "Y"));
        node.getLogEntries().addAll(originalLog);
        node.setCommitIndex(2);
        node.setLastHeartbeat(100);
        service = new RaftService(node, mock(ReplicationService.class));
    }

    @Test
    void staleLeaderCannotChangeLogCommitIndexOrTimer() {
        assertEquals(new AppendEntriesResponse(false, 3),
                service.appendEntries(new AppendEntries(2, 2, 2, 1, List.of(new LogEntry(2, "C")), 3)));
        assertEquals(originalLog, node.getLogEntries());
        assertEquals(2, node.getCommitIndex());
        assertEquals(100, node.getLastHeartbeat());
    }

    @Test
    void missingPreviousEntryRejectsRequestButRefreshesLeaderTimer() {
        assertFalse(service.appendEntries(new AppendEntries(3, 2, 5, 2, List.of(), 0)).success());
        assertEquals(originalLog, node.getLogEntries());
        assertTrue(node.getLastHeartbeat() > 100);
    }

    @Test
    void mismatchingPreviousTermDoesNotTruncateLog() {
        assertFalse(service.appendEntries(new AppendEntries(3, 2, 3, 1, List.of(new LogEntry(3, "D")), 0)).success());
        assertEquals(originalLog, node.getLogEntries());
    }

    @Test
    void negativePreviousIndexIsRejected() {
        assertFalse(service.appendEntries(new AppendEntries(3, 2, -1, 0, List.of(), 0)).success());
        assertEquals(originalLog, node.getLogEntries());
    }

    @Test
    void appendsMissingEntriesWithoutChangingExistingPrefix() {
        assertTrue(service.appendEntries(new AppendEntries(3, 2, 4, 2, List.of(new LogEntry(3, "E")), 2)).success());
        assertEquals(List.of(originalLog.get(0), originalLog.get(1), originalLog.get(2), originalLog.get(3),
                new LogEntry(3, "E")), node.getLogEntries());
    }

    @Test
    void replacesConflictAndFollowingTailAndHandlesDuplicateRequest() {
        AppendEntries request = new AppendEntries(3, 2, 2, 1,
                List.of(new LogEntry(3, "C"), new LogEntry(3, "D")), 4);

        assertTrue(service.appendEntries(request).success());
        assertTrue(service.appendEntries(request).success());

        assertEquals(List.of(new LogEntry(1, "A"), new LogEntry(1, "B"),
                new LogEntry(3, "C"), new LogEntry(3, "D")), node.getLogEntries());
        assertEquals(4, node.getCommitIndex());
    }

    @Test
    void heartbeatDoesNotDeleteUnverifiedTailOrCommitBeyondItsPreviousIndex() {
        assertTrue(service.appendEntries(new AppendEntries(3, 2, 2, 1, List.of(), 4)).success());
        assertEquals(originalLog, node.getLogEntries());
        assertEquals(2, node.getCommitIndex());
    }

    @Test
    void matchingShortBatchPreservesLongerSuffixAndCommittedIndex() {
        node.setCommitIndex(4);

        assertTrue(service.appendEntries(new AppendEntries(3, 2, 0, 0,
                List.of(new LogEntry(1, "A")), 1)).success());
        assertEquals(originalLog, node.getLogEntries());
        assertEquals(4, node.getCommitIndex());
    }

    @Test
    void commitIndexIsLimitedToLastMatchedEntry() {
        assertTrue(service.appendEntries(new AppendEntries(3, 2, 2, 1, List.of(new LogEntry(3, "C")), 100)).success());
        assertEquals(3, node.getLastLogIndex());
        assertEquals(3, node.getCommitIndex());
    }

    @Test
    void heartbeatCanAdvanceCommitIndexWithoutNewEntries() {
        assertTrue(service.appendEntries(new AppendEntries(3, 2, 4, 2, List.of(), 3)).success());
        assertEquals(originalLog, node.getLogEntries());
        assertEquals(3, node.getCommitIndex());
    }

    @Test
    void emptyFollowerHintsFirstIndexInsteadOfRequiringOneProbePerMissingEntry() {
        node.getLogEntries().clear();
        node.setCommitIndex(0);

        assertEquals(new AppendEntriesResponse(false, 3, 1),
                service.appendEntries(new AppendEntries(3, 2, 1000, 3, List.of(), 0)));
    }

    @Test
    void termConflictHintPointsToFirstEntryOfTheConflictingTerm() {
        assertEquals(new AppendEntriesResponse(false, 3, 3),
                service.appendEntries(new AppendEntries(3, 2, 4, 1, List.of(), 0)));
    }

    @Test
    void logsStoredReplicationOnceAndDoesNotLogDuplicatesOrHeartbeatsAsNewData() {
        AppendEntries request = new AppendEntries(3, 2, 2, 1,
                List.of(new LogEntry(3, "C"), new LogEntry(3, "D")), 4);
        try (LogCapture capture = new LogCapture(RaftService.class)) {
            service.appendEntries(request);
            service.appendEntries(request);
            service.appendEntries(new AppendEntries(3, 2, 4, 3, List.of(), 4));

            List<String> replicationMessages = capture.messages().stream()
                    .filter(message -> message.contains("Replicated entries from leader"))
                    .toList();
            assertEquals(1, replicationMessages.size());
            assertTrue(replicationMessages.getFirst().contains("firstIndex=3, lastIndex=4, count=2"));
            List<String> commitMessages = capture.messages().stream()
                    .filter(message -> message.contains("Committed log through index"))
                    .toList();
            assertEquals(List.of("[Node 1]: Committed log through index=4, term=3"), commitMessages);
            assertEquals(4, node.getCommitIndex());
        }
    }

    @Test
    void higherTermStepsLeaderDownEvenWhenLogDoesNotMatch() {
        node.setStatus(NodeStatus.CANDIDATE);
        node.becomeLeader();
        node.setVotedFor(1);

        assertEquals(new AppendEntriesResponse(false, 4, 5),
                service.appendEntries(new AppendEntries(4, 2, 5, 3, List.of(), 0)));
        assertEquals(NodeStatus.FOLLOWER, node.getStatus());
        assertEquals(4, node.getCurrentTerm().get());
        assertNull(node.getVotedFor());
        assertEquals(originalLog, node.getLogEntries());
    }
}
