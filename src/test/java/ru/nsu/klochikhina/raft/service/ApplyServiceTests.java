package ru.nsu.klochikhina.raft.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ru.nsu.klochikhina.raft.RaftNode;
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

    private RaftNode node;
    private StringStateMachine stateMachine;
    private ApplyService service;

    @BeforeEach
    void setUp() {
        node = node(3);
        node.getLogEntries().addAll(List.of(
                new LogEntry(1, "A"), new LogEntry(2, "B"),
                new LogEntry(2, "C"), new LogEntry(3, "D")
        ));
        stateMachine = new StringStateMachine();
        service = new ApplyService(node, stateMachine);
    }

    @Test
    void appliesCommittedPrefixOnceInOrderAndLogsOnlySuccessfulApplications() {
        node.setCommitIndex(3);
        try (LogCapture capture = new LogCapture(ApplyService.class)) {
            service.applyCommittedEntries();
            service.applyCommittedEntries();

            assertEquals(List.of("A", "B", "C"), stateMachine.commands());
            assertEquals(3, node.getLastApplied());
            assertEquals(3, capture.messages().size());
            assertTrue(capture.messages().get(0).contains("index=1, term=1, command=A"));
            assertTrue(capture.messages().get(2).contains("index=3, term=2, command=C"));

            node.setCommitIndex(4);
            service.applyCommittedEntries();
            assertEquals(List.of("A", "B", "C", "D"), stateMachine.commands());
            assertEquals(4, node.getLastApplied());
            assertEquals(4, capture.messages().size());
        }
    }

    @Test
    void doesNotApplyOrLogUncommittedEntries() {
        try (LogCapture capture = new LogCapture(ApplyService.class)) {
            service.applyCommittedEntries();

            assertEquals(List.of(), stateMachine.commands());
            assertEquals(0, node.getLastApplied());
            assertTrue(capture.messages().isEmpty());
        }
    }

    @Test
    void failedApplicationDoesNotAdvanceCursorAndRetryDoesNotRepeatSuccessfulPrefix() {
        List<String> applied = new ArrayList<>();
        AtomicBoolean failOnce = new AtomicBoolean(true);
        StateMachine machine = command -> {
            if (command.equals("B") && failOnce.getAndSet(false)) {
                throw new IllegalStateException("temporary state machine failure");
            }
            applied.add(command);
        };
        ApplyService application = new ApplyService(node, machine);
        node.setCommitIndex(3);

        try (LogCapture capture = new LogCapture(ApplyService.class)) {
            assertThrows(IllegalStateException.class, application::applyCommittedEntries);
            assertEquals(1, node.getLastApplied());
            assertEquals(List.of("A"), applied);
            assertEquals(1, capture.messages().size());

            application.applyCommittedEntries();

            assertEquals(3, node.getLastApplied());
            assertEquals(List.of("A", "B", "C"), applied);
            assertEquals(3, capture.messages().size());
        }
    }
}
