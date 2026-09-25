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

    @PostMapping("/request-vote")
    public VoteResponse vote(@RequestBody RequestVote requestVote) {
        return raftService.vote(requestVote);
    }

    @PostMapping("/append-entries")
    public AppendEntriesResponse heartbeat(@RequestBody AppendEntries appendEntries) {
        return raftService.heartbeat(appendEntries);
    }
}
