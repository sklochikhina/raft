package ru.nsu.klochikhina.raft.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.nsu.klochikhina.raft.dto.NodeInfoResponse;
import ru.nsu.klochikhina.raft.dto.PingResponse;
import ru.nsu.klochikhina.raft.dto.RequestVote;
import ru.nsu.klochikhina.raft.dto.VoteResponse;
import ru.nsu.klochikhina.raft.service.RaftService;

@RequiredArgsConstructor
@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftService raftService;

    @GetMapping("/node/info")
    public NodeInfoResponse getNodeInfo() {
        return raftService.getNodeInfo();
    }

    @GetMapping("/ping")
    public PingResponse ping() {
        return raftService.ping();
    }

    @PostMapping("/request/vote")
    public VoteResponse vote(@RequestBody RequestVote requestVote) {
        return raftService.vote(requestVote);
    }
}
