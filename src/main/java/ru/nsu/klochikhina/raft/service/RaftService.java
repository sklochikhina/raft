package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.*;
import ru.nsu.klochikhina.raft.model.NodeInfo;
import ru.nsu.klochikhina.raft.model.NodeStatus;

import java.util.List;

@Service
@RequiredArgsConstructor
public class RaftService {

    private final RaftNode raftNode;

    public StatusResponse status() {
        int nodeId = raftNode.getConfig().getNodeId();
        List<NodeInfo> allNodes = raftNode.getClusterProperties().getNodes();

        NodeInfo self = allNodes.stream()
                .filter(n -> n.id() == nodeId)
                .findFirst()
                .orElse(new NodeInfo(nodeId, raftNode.getConfig().getPort(), "unknown"));

        List<NodeInfo> peers = allNodes.stream()
                .filter(n -> n.id() != nodeId)
                .toList();

        return new StatusResponse(
                self,
                raftNode.getStatus(),
                raftNode.getCurrentTerm().get(),
                peers
        );
    }

    public RequestVoteResponse vote(RequestVote requestVote) {
        synchronized (raftNode) {
            long currentTerm = raftNode.getCurrentTerm().get();

            if (currentTerm > requestVote.term()) {
                return new RequestVoteResponse(false, currentTerm);
            }

            if (requestVote.term() > currentTerm) {
                raftNode.becomeFollower(requestVote.term());
            }

            boolean canVoteForCandidate = raftNode.getVotedFor() == null
                    || raftNode.getVotedFor().equals(requestVote.candidateId());

            // TODO: implement log up-to-date check
            //boolean isLogUpToDate = isLogUpToDate(requestVote.lastLogTerm(), requestVote.lastLogIndex());

            if (canVoteForCandidate /*&& isLogUpToDate*/) {
                raftNode.setVotedFor(requestVote.candidateId());
                raftNode.resetElectionTimer();
                return new RequestVoteResponse(true, raftNode.getCurrentTerm().get());
            }

            return new RequestVoteResponse(false, raftNode.getCurrentTerm().get());
        }
    }

    public AppendEntriesResponse heartbeat(AppendEntries appendEntries) {
        synchronized (raftNode) {
            long currentTerm = raftNode.getCurrentTerm().get();

            if (appendEntries.term() < currentTerm) {
                return new AppendEntriesResponse(false, currentTerm);
            }

            if (appendEntries.term() > currentTerm || raftNode.getStatus() != NodeStatus.FOLLOWER) {
                raftNode.becomeFollower(appendEntries.term());
            }

            raftNode.resetElectionTimer();

            return new AppendEntriesResponse(true, raftNode.getCurrentTerm().get());
        }
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
