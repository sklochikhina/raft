package ru.nsu.klochikhina.raft.dto;

import java.util.List;

public record AddEntries(
        List<String> entries
) {
}
