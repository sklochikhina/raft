package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.model.NodeStatus;

@Service
@RequiredArgsConstructor
public class ElectionTimer {

    private final ElectionService electionService;
    private final RaftNode raftNode;

    @Scheduled(fixedDelay = 20)
    public void checkElectionTimeout() {
        if (raftNode.getStatus() == NodeStatus.LEADER) {
            return;
        }

        long elapsed = System.currentTimeMillis() - raftNode.getLastHeartbeat();

        if (elapsed > raftNode.getElectionTimeout()) {
            electionService.startElection();
        }
    }
}
