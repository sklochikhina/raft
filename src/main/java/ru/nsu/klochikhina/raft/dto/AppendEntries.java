package ru.nsu.klochikhina.raft.dto;

import ru.nsu.klochikhina.raft.model.LogEntry;

import java.util.List;

public record AppendEntries(
        long term,
        int leaderId,
        int prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        int leaderCommit
) {
}
