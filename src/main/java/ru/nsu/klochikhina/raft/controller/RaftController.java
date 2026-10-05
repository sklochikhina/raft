package ru.nsu.klochikhina.raft.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.nsu.klochikhina.raft.dto.*;
import ru.nsu.klochikhina.raft.service.RaftService;

@RequiredArgsConstructor
@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftService raftService;

    @GetMapping("/status")
    private StatusResponse status() {
        return raftService.status();
    }

    @PostMapping("/request_vote")
    public VoteResponse vote(@RequestBody RequestVote requestVote) {
        return raftService.vote(requestVote);
    }

    @PostMapping("/append_entries")
    public AppendEntriesResponse heartbeat(@RequestBody AppendEntries appendEntries) {
        return raftService.heartbeat(appendEntries);
    }
}
