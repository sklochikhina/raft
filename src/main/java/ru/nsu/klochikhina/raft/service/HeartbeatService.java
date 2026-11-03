package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class HeartbeatService {

    private final ReplicationService replicationService;

    @Scheduled(fixedDelay = 100)
    public void start() {
        replicationService.replicateLog();
    }
}
