package ru.nsu.klochikhina.raft.dto;

public record RequestVoteResponse(
        boolean voteGranted,
        long term
) {
}
