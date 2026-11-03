package ru.nsu.klochikhina.raft.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.nsu.klochikhina.raft.dto.*;
import ru.nsu.klochikhina.raft.service.RaftService;

@RequiredArgsConstructor
@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftService raftService;

    @GetMapping("/status")
    public StatusResponse status() {
        return raftService.status();
    }

    @PostMapping("/add_entries")
    public ResponseEntity<AddEntriesResponse> addEntries(@RequestBody AddEntries entries) {
        AddEntriesResponse response = raftService.addEntries(entries);
        return ResponseEntity.status(response.status()).body(response);
    }

    @PostMapping("/request_vote")
    public RequestVoteResponse requestVote(@RequestBody RequestVote requestVote) {
        return raftService.vote(requestVote);
    }

    @PostMapping("/append_entries")
    public AppendEntriesResponse appendEntries(@RequestBody AppendEntries appendEntries) {
        return raftService.appendEntries(appendEntries);
    }
}
