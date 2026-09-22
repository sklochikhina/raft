package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.dto.NodeInfoResponse;
import ru.nsu.klochikhina.raft.dto.PingResponse;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.RequestVote;
import ru.nsu.klochikhina.raft.dto.VoteResponse;

import java.util.Objects;

@Service
@RequiredArgsConstructor
public class RaftService {

    private final RaftNode raftNode;

    public NodeInfoResponse getNodeInfo() {
        return raftNode.nodeInfo();
    }

    public PingResponse ping() {
        return new PingResponse(raftNode.nodeInfo().nodeId(), "pong");
    }

    public VoteResponse vote(RequestVote requestVote) {
        if (raftNode.getCurrentTerm() < requestVote.term() && raftNode.getVotedFor() == null) {
            raftNode.setVotedFor(requestVote.candidateId());
            raftNode.setCurrentTerm(requestVote.term());
        }

        return new VoteResponse(
                Objects.equals(raftNode.getVotedFor(), requestVote.candidateId()),
                raftNode.getCurrentTerm()
        );
    }
}
