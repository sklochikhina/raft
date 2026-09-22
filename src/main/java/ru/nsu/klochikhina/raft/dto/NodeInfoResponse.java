package ru.nsu.klochikhina.raft.dto;

import ru.nsu.klochikhina.raft.model.NodeStatus;

public record NodeInfoResponse(
        Integer nodeId,
        Integer port,
        NodeStatus status,
        Integer currentTerm
) {
}
