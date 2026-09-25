package ru.nsu.klochikhina.raft.dto;

public record RequestVote(
        long newTerm,
        int candidateId
) {
}
