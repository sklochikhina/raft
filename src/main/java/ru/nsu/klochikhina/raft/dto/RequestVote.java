package ru.nsu.klochikhina.raft.dto;

public record RequestVote(
        long term,
        int candidateId
) {
}
