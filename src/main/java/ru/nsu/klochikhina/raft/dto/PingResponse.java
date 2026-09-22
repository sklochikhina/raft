package ru.nsu.klochikhina.raft.dto;

import lombok.NonNull;

public record PingResponse(
        @NonNull Integer nodeId,
        @NonNull String message
) {
}
