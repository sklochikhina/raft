package ru.nsu.klochikhina.raft.service;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.CommandResult;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.statemachine.StateMachine;
import ru.nsu.klochikhina.raft.statemachine.StringStateMachine;
import ru.nsu.klochikhina.raft.support.LogCapture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static ru.nsu.klochikhina.raft.support.RaftTestFixture.node;

class ApplyServiceTests {

    @Test
    void appliesOnlyCommittedPrefixInOrderAndDoesNotRepeatIt() {
        RaftNode node = node(3);
        node.getLogEntries().addAll(List.of(new LogEntry(1, 1, "A"),
                new LogEntry(2, 1, "B"), new LogEntry(3, 2, "C")));
        node.setCommitIndex(2);
        StringStateMachine machine = new StringStateMachine();
        ApplyService service = new ApplyService(node, machine);

        try (LogCapture capture = new LogCapture(ApplyService.class, Level.DEBUG)) {
            service.applyCommittedEntries();
            List<CommandResult> snapshot = service.resultsFor(1, 2);
            service.applyCommittedEntries();
            assertEquals(List.of("A", "B"), machine.commands());
            assertEquals(2, node.getLastApplied());
            assertEquals(List.of(new CommandResult(1, "applied: A"), new CommandResult(2, "applied: B")), snapshot);
            assertEquals(2, capture.messages().size());

            node.setCommitIndex(3);
            service.applyCommittedEntries();
            assertEquals(List.of("A", "B", "C"), machine.commands());
            assertEquals(3, node.getLastApplied());
            assertEquals(List.of(new CommandResult(3, "applied: C")), service.resultsFor(3, 3));
            assertEquals(2, snapshot.size());
            assertThrows(UnsupportedOperationException.class, snapshot::clear);
            assertEquals(3, capture.messages().size());
        }
    }

    @Test
    void appliesCommandsWithoutPerEntryMessagesAtInfoAndRestoresLoggerLevel() {
        RaftNode node = node(3);
        node.getLogEntries().add(new LogEntry(1, 1, "A"));
        node.setCommitIndex(1);
        StringStateMachine machine = new StringStateMachine();
        ApplyService service = new ApplyService(node, machine);
        Logger logger = (Logger) LoggerFactory.getLogger(ApplyService.class);
        Level previousLevel = logger.getLevel();

        try (LogCapture capture = new LogCapture(ApplyService.class, Level.INFO)) {
            service.applyCommittedEntries();
            assertEquals(List.of("A"), machine.commands());
            assertEquals(1, node.getLastApplied());
            assertTrue(capture.messages().isEmpty());
        }

        assertEquals(previousLevel, logger.getLevel());
    }

    @Test
    void doesNotApplyUncommittedEntries() {
        RaftNode node = node(3);
        node.getLogEntries().add(new LogEntry(1, 1, "A"));
        StringStateMachine machine = new StringStateMachine();
        ApplyService service = new ApplyService(node, machine);

        service.applyCommittedEntries();

        assertTrue(machine.commands().isEmpty());
        assertEquals(0, node.getLastApplied());
    }

    @Test
    void failedCommandStopsApplicationAndRetryDoesNotRepeatSuccessfulPrefix() {
        RaftNode node = node(3);
        node.getLogEntries().addAll(List.of(new LogEntry(1, 1, "A"), new LogEntry(2, 1, "B")));
        node.setCommitIndex(2);
        List<String> commands = new ArrayList<>();
        AtomicBoolean fail = new AtomicBoolean(true);
        StateMachine machine = command -> {
            if (command.equals("B") && fail.get()) {
                throw new IllegalStateException("temporary failure");
            }
            commands.add(command);
            return "applied: " + command;
        };
        ApplyService service = new ApplyService(node, machine);

        service.applyCommittedEntries();
        assertEquals(1, node.getLastApplied());
        assertEquals(List.of("A"), commands);
        assertEquals(List.of(new CommandResult(1, "applied: A")), service.resultsFor(1, 1));

        fail.set(false);
        service.applyCommittedEntries();
        assertEquals(2, node.getLastApplied());
        assertEquals(List.of("A", "B"), commands);
        assertEquals(List.of(new CommandResult(2, "applied: B")), service.resultsFor(2, 2));
    }
}
