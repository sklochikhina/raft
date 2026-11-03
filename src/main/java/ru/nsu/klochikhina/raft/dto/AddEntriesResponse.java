package ru.nsu.klochikhina.raft.dto;

import org.springframework.http.HttpStatus;

public record AddEntriesResponse(
        HttpStatus status,
        boolean committed
) {
}
