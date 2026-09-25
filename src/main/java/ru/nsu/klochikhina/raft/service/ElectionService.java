package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.RequestVote;

@Service
@RequiredArgsConstructor
public class ElectionService {

    private final RaftNode raftNode;
    private final NetworkService networkService;

    public void startElection() {
        raftNode.becomeCandidate();
        raftNode.resetElectionTimer();
        networkService.sendRequestVote(
                new RequestVote(raftNode.getCurrentTerm().get(), raftNode.getConfig().getNodeId())
        );

    }
}
