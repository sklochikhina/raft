package ru.nsu.klochikhina.raft.dto;

import ru.nsu.klochikhina.raft.model.NodeInfo;
import ru.nsu.klochikhina.raft.model.NodeStatus;

import java.util.List;

public record StatusResponse(
        NodeInfo nodeInfo,
        NodeStatus status,
        long currentTerm,
        List<NodeInfo> peers
) {
}
