package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.AppendEntries;
import ru.nsu.klochikhina.raft.model.NodeStatus;

@Service
@RequiredArgsConstructor
public class HeartbeatTimer {

    private final NetworkService networkService;
    private final RaftNode raftNode;

    @Scheduled(fixedDelay = 20)
    public void start() {
        if (raftNode.getStatus() != NodeStatus.LEADER) {
            return;
        }

        var appendEntries = new AppendEntries(raftNode.getCurrentTerm().get(), raftNode.getConfig().getNodeId());

        networkService.sendAppendEntries(appendEntries);
    }
}
