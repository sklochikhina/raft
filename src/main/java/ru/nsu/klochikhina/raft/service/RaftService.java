package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.dto.NodeInfo;
import ru.nsu.klochikhina.raft.model.RaftNode;

@Service
@RequiredArgsConstructor
public class RaftService {

    private final RaftNode raftNode;

    public NodeInfo getNodeInfo() {
        return raftNode.nodeInfo();
    }
}
