package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.RequestVote;
import ru.nsu.klochikhina.raft.model.NodeStatus;

@Service
@RequiredArgsConstructor
public class ElectionService {

    private final RaftNode raftNode;
    private final NetworkService networkService;

    @Scheduled(fixedDelay = 50)
    public void checkElectionTimeout() {
        if (raftNode.getStatus() == NodeStatus.LEADER) {
            return;
        }

        long elapsed = System.currentTimeMillis() - raftNode.getLastHeartbeat();

        if (elapsed > raftNode.getElectionTimeout()) {
            startElection();
        }
    }

    private void startElection() {
        raftNode.becomeCandidate();
        raftNode.resetElectionTimer();
        networkService.sendRequestVote(
                new RequestVote(raftNode.getCurrentTerm().get(), raftNode.getConfig().getNodeId())
        );
    }
}
