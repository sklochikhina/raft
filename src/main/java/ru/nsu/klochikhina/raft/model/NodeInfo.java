package ru.nsu.klochikhina.raft.model;

public record NodeInfo(
        int id,
        int port,
        String host
) {
}
