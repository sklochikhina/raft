package ru.nsu.klochikhina.raft.dto;

public record AppendEntriesResponse(
        boolean success,
        long term
) {
}
