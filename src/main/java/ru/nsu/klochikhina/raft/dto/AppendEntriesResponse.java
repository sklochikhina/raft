package ru.nsu.klochikhina.raft.dto;

public record AppendEntriesResponse(
        boolean success,
        long term,
        Integer conflictIndex
) {
    public AppendEntriesResponse(boolean success, long term) {
        this(success, term, null);
    }
}
