package ru.nsu.klochikhina.raft.model;

public record NodeInfo(
        Integer id,
        Integer port,
        String host
) {
}
