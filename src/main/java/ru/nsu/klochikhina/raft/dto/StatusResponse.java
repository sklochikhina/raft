package ru.nsu.klochikhina.raft.dto;

import ru.nsu.klochikhina.raft.model.NodeInfo;
import ru.nsu.klochikhina.raft.model.NodeStatus;
import ru.nsu.klochikhina.raft.model.LogEntry;

import java.util.List;

public record StatusResponse(
        NodeInfo nodeInfo,
        NodeStatus status,
        long currentTerm,
        List<NodeInfo> peers,
        List<LogEntry> entries,
        int commitIndex,
        int lastApplied
) {
}
