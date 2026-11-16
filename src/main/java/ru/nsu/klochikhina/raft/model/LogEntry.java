package ru.nsu.klochikhina.raft.model;

public record LogEntry(
        int index,
        long term,
        String command
) {
}
