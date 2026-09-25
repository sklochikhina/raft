package ru.nsu.klochikhina.raft.dto;

public record VoteResponse(
        boolean voteGranted,
        long currentTerm
) {
}
