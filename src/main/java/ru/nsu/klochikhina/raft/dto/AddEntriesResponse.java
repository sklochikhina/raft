package ru.nsu.klochikhina.raft.dto;

import org.springframework.http.HttpStatus;

import java.util.List;

public record AddEntriesResponse(
        HttpStatus status,
        boolean committed,
        boolean applied,
        List<CommandResult> results
) {
}
