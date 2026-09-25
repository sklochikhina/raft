package ru.nsu.klochikhina.raft.dto;

public record AppendEntriesResponse(
        boolean isSuccess,
        long currentTerm
) {
}
