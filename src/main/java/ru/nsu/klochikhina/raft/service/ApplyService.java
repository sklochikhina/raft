package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.CommandResult;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.statemachine.StateMachine;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class ApplyService {

    private final RaftNode raftNode;
    private final StateMachine stateMachine;
    private final List<CommandResult> results = new ArrayList<>();

    public void applyCommittedEntries() {
        synchronized (raftNode) {
            while (raftNode.getLastApplied() < raftNode.getCommitIndex()) {
                int index = raftNode.getLastApplied() + 1;
                LogEntry entry = raftNode.getLogEntries().get(index - 1);
                String result;
                try {
                    result = stateMachine.apply(entry.command());
                } catch (RuntimeException e) {
                    log.error("[Node {}]: Failed to apply entry: index={}, term={}",
                            raftNode.getConfig().getNodeId(), index, entry.term(), e);
                    return;
                }
                results.add(new CommandResult(index, result));
                raftNode.setLastApplied(index);
                log.debug("[Node {}]: Applied entry to state machine: index={}, term={}, command={}",
                        raftNode.getConfig().getNodeId(), index, entry.term(), entry.command());
            }
        }
    }

    public List<CommandResult> resultsFor(int firstIndex, int lastIndex) {
        synchronized (raftNode) {
            return List.copyOf(results.subList(firstIndex - 1, lastIndex));
        }
    }
}
