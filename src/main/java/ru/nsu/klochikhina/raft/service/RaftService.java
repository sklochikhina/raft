package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.*;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.model.NodeInfo;
import ru.nsu.klochikhina.raft.model.NodeStatus;

import java.util.List;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class RaftService {

    private final RaftNode raftNode;
    private final ReplicationService replicationService;

    public StatusResponse status() {
        synchronized (raftNode) {
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
                    peers,
                    List.copyOf(raftNode.getLogEntries()),
                    raftNode.getCommitIndex()
            );
        }
    }

    public AddEntriesResponse addEntries(AddEntries entries) {
        if (entries == null || entries.entries() == null || entries.entries().isEmpty()
                || entries.entries().stream().anyMatch(Objects::isNull)) {
            return new AddEntriesResponse(HttpStatus.BAD_REQUEST, false);
        }

        List<String> commands = List.copyOf(entries.entries());
        int lastIndex;
        LogEntry lastEntry;
        synchronized (raftNode) {
            if (raftNode.getStatus() != NodeStatus.LEADER) {
                return new AddEntriesResponse(HttpStatus.CONFLICT, false);
            }

            long term = raftNode.getCurrentTerm().get();
            List<LogEntry> batch = commands.stream()
                    .map(command -> new LogEntry(term, command))
                    .toList();
            raftNode.getLogEntries().addAll(batch);
            lastIndex = raftNode.getLastLogIndex();
            lastEntry = batch.getLast();
        }

        replicationService.replicateLog();

        synchronized (raftNode) {
            boolean committed = raftNode.getCommitIndex() >= lastIndex
                    && raftNode.getLastLogIndex() >= lastIndex
                    && raftNode.getLogEntries().get(lastIndex - 1).equals(lastEntry);
            return new AddEntriesResponse(committed ? HttpStatus.OK : HttpStatus.ACCEPTED, committed);
        }
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

            boolean isLogUpToDate = isLogUpToDate(requestVote.lastLogTerm(), requestVote.lastLogIndex());

            if (canVoteForCandidate && isLogUpToDate) {
                raftNode.setVotedFor(requestVote.candidateId());
                raftNode.resetElectionTimer();
                return new RequestVoteResponse(true, raftNode.getCurrentTerm().get());
            }

            return new RequestVoteResponse(false, raftNode.getCurrentTerm().get());
        }
    }

    public AppendEntriesResponse appendEntries(AppendEntries appendEntries) {
        synchronized (raftNode) {
            long currentTerm = raftNode.getCurrentTerm().get();

            if (appendEntries.term() < currentTerm) {
                return new AppendEntriesResponse(false, currentTerm);
            }

            if (appendEntries.term() > currentTerm || raftNode.getStatus() != NodeStatus.FOLLOWER) {
                raftNode.becomeFollower(appendEntries.term());
            }

            raftNode.resetElectionTimer();

            int prevLogIndex = appendEntries.prevLogIndex();
            if (prevLogIndex < 0) {
                return new AppendEntriesResponse(false, raftNode.getCurrentTerm().get());
            }
            if (prevLogIndex > raftNode.getLastLogIndex()) {
                return new AppendEntriesResponse(false, raftNode.getCurrentTerm().get(), raftNode.getLastLogIndex() + 1);
            }
            if (raftNode.getLogTerm(prevLogIndex) != appendEntries.prevLogTerm()) {
                long conflictingTerm = raftNode.getLogTerm(prevLogIndex);
                int conflictIndex = prevLogIndex;
                while (conflictIndex > 1 && raftNode.getLogTerm(conflictIndex - 1) == conflictingTerm) {
                    conflictIndex--;
                }
                return new AppendEntriesResponse(false, raftNode.getCurrentTerm().get(), Math.max(1, conflictIndex));
            }

            List<LogEntry> localLog = raftNode.getLogEntries();

            int index = prevLogIndex + 1;
            int firstAddedIndex = 0;

            for (LogEntry incomingEntry : appendEntries.entries()) {
                if (index <= localLog.size()) {
                    LogEntry existingEntry = localLog.get(index - 1);

                    if (existingEntry.term() == incomingEntry.term()) {
                        index++;
                        continue;
                    }

                    localLog.subList(index - 1, localLog.size()).clear();
                }

                localLog.add(incomingEntry);
                if (firstAddedIndex == 0) {
                    firstAddedIndex = index;
                }
                index++;
            }

            if (firstAddedIndex > 0) {
                log.info("[Node {}]: Replicated entries from leader {}: firstIndex={}, lastIndex={}, count={}, term={}",
                        raftNode.getConfig().getNodeId(), appendEntries.leaderId(), firstAddedIndex, index - 1,
                        index - firstAddedIndex, raftNode.getCurrentTerm().get());
            }

            int lastMatchedIndex = prevLogIndex + appendEntries.entries().size();

            int newCommitIndex = Math.min(appendEntries.leaderCommit(), lastMatchedIndex);

            if (newCommitIndex > raftNode.getCommitIndex()) {
                raftNode.setCommitIndex(newCommitIndex);
                log.info("[Node {}]: Committed log through index={}, term={}",
                        raftNode.getConfig().getNodeId(), newCommitIndex, raftNode.getCurrentTerm().get());
            }

            return new AppendEntriesResponse(true, raftNode.getCurrentTerm().get());
        }
    }

    private boolean isLogUpToDate(long candidateLastLogTerm, int candidateLastLogIndex) {
        int myLastLogIndex = raftNode.getLastLogIndex();
        long myLastLogTerm = raftNode.getLogTerm(myLastLogIndex);

        if (candidateLastLogTerm != myLastLogTerm) {
            return candidateLastLogTerm > myLastLogTerm;
        }
        return candidateLastLogIndex >= myLastLogIndex;
    }
}
