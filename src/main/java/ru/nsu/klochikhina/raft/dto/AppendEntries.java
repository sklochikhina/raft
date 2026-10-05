package ru.nsu.klochikhina.raft.dto;

public record AppendEntries(
        long term,
        int leaderId
) {
}
