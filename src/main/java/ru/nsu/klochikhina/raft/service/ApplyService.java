package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.statemachine.StateMachine;

@Service
@RequiredArgsConstructor
@Slf4j
public class ApplyService {

    private final RaftNode raftNode;
    private final StateMachine stateMachine;

    public void applyCommittedEntries() {
        synchronized (raftNode) {
            while (raftNode.getLastApplied() < raftNode.getCommitIndex()) {
                int index = raftNode.getLastApplied() + 1;
                LogEntry entry = raftNode.getLogEntries().get(index - 1);
                stateMachine.apply(entry.command());
                raftNode.setLastApplied(index);
                log.info("[Node {}]: Applied entry to state machine: index={}, term={}, command={}",
                        raftNode.getConfig().getNodeId(), index, entry.term(), entry.command());
            }
        }
    }
}
