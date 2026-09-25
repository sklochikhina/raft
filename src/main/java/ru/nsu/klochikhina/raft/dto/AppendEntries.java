package ru.nsu.klochikhina.raft.dto;

public record AppendEntries(
        long currentTerm,
        int leaderId
) {
}
