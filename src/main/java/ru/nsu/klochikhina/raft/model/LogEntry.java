package ru.nsu.klochikhina.raft.model;

public record LogEntry(
        long term,
        String command
) {
}
