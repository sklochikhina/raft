package ru.nsu.klochikhina.raft.dto;

public record RequestVote(
        Integer term,
        Integer candidateId
) {
}
