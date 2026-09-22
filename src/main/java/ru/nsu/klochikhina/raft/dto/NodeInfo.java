package ru.nsu.klochikhina.raft.dto;

public record NodeInfo(Integer nodeId, Integer port, ru.nsu.klochikhina.raft.model.NodeStatus status, Integer currentTerm) {
}
