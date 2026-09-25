package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.*;
import ru.nsu.klochikhina.raft.model.NodeStatus;

@Service
@RequiredArgsConstructor
public class RaftService {

    private final RaftNode raftNode;

    public VoteResponse vote(RequestVote requestVote) {

        if (raftNode.getCurrentTerm().get() > requestVote.newTerm()) {
            return new VoteResponse(false, raftNode.getCurrentTerm().get());
        }

        if (requestVote.newTerm() > raftNode.getCurrentTerm().get()) {
            raftNode.becomeFollower(requestVote.newTerm());
        }

        boolean canVoteForCandidate = raftNode.getVotedFor() == null
                || raftNode.getVotedFor().equals(requestVote.candidateId());

        // TODO: implement log up-to-date check
        //boolean isLogUpToDate = isLogUpToDate(requestVote.lastLogTerm(), requestVote.lastLogIndex());

        if (canVoteForCandidate /*&& isLogUpToDate*/) {
            raftNode.setVotedFor(requestVote.candidateId());
            raftNode.setStatus(NodeStatus.FOLLOWER);
            raftNode.resetElectionTimer();
            return new VoteResponse(true, raftNode.getCurrentTerm().get());
        }

        return new VoteResponse(false, raftNode.getCurrentTerm().get());
    }

    public AppendEntriesResponse heartbeat(AppendEntries appendEntries) {
        long currentTerm = raftNode.getCurrentTerm().get();

        if (appendEntries.currentTerm() < currentTerm) {
            return new AppendEntriesResponse(false, currentTerm);
        }

        if (appendEntries.currentTerm() > currentTerm) {
            raftNode.getCurrentTerm().set(appendEntries.currentTerm());
            raftNode.setVotedFor(null);
        }

        raftNode.setStatus(NodeStatus.FOLLOWER);
        raftNode.resetElectionTimer();

        return new AppendEntriesResponse(true, raftNode.getCurrentTerm().get());
    }

    /*private boolean isLogUpToDate(long candidateLastLogTerm, long candidateLastLogIndex) {
        long myLastLogTerm = raftNode.getLog().getLastLogTerm();
        long myLastLogIndex = raftNode.getLog().getLastLogIndex();

        if (candidateLastLogTerm != myLastLogTerm) {
            return candidateLastLogTerm > myLastLogTerm;
        }
        return candidateLastLogIndex >= myLastLogIndex;
    }*/
}
