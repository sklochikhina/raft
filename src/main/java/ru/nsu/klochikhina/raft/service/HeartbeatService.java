package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.AppendEntries;
import ru.nsu.klochikhina.raft.model.NodeStatus;

@Service
@RequiredArgsConstructor
public class HeartbeatService {

    private final NetworkService networkService;
    private final RaftNode raftNode;

    @Scheduled(fixedDelay = 50)
    public void start() {
        if (raftNode.getStatus() != NodeStatus.LEADER) {
            return;
        }
        networkService.sendAppendEntries(
                new AppendEntries(raftNode.getCurrentTerm().get(), raftNode.getConfig().getNodeId())
        );
    }
}
