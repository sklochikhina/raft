package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.AppendEntries;
import ru.nsu.klochikhina.raft.dto.AppendEntriesResponse;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.model.NodeInfo;
import ru.nsu.klochikhina.raft.model.NodeStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class ReplicationService {

    private final RaftNode raftNode;
    private final NetworkService networkService;

    private final Map<Integer, AppendEntries> pendingRequests = new HashMap<>();
    private final Map<Integer, Long> recoveringFollowers = new HashMap<>();

    public void replicateLog() {
        synchronized (raftNode) {
            if (raftNode.getStatus() != NodeStatus.LEADER) {
                return;
            }
            advanceCommitIndex();
        }

        for (NodeInfo follower : raftNode.getClusterProperties().getNodes()) {
            AppendEntries request;

            synchronized (raftNode) {
                if (raftNode.getStatus() != NodeStatus.LEADER) {
                    return;
                }
                if (follower.id() == raftNode.getConfig().getNodeId()) {
                    continue;
                }

                AppendEntries pending = pendingRequests.get(follower.id());
                if (pending != null && pending.term() == raftNode.getCurrentTerm().get()) {
                    continue;
                }

                int nextIndex = isRecovering(follower.id())
                        ? 1 : raftNode.getNextIndex().get(follower.id());
                int prevLogIndex = nextIndex - 1;
                List<LogEntry> log = raftNode.getLogEntries();

                List<LogEntry> entries = List.copyOf(
                        log.subList(nextIndex - 1, log.size())
                );

                request = new AppendEntries(
                        raftNode.getCurrentTerm().get(),
                        raftNode.getConfig().getNodeId(),
                        prevLogIndex,
                        raftNode.getLogTerm(prevLogIndex),
                        entries,
                        raftNode.getCommitIndex()
                );
                pendingRequests.put(follower.id(), request);
            }

            try {
                networkService.sendAppendEntries(follower, request)
                        .whenComplete((response, error) ->
                                handleResponse(follower.id(), request, response, error));
            } catch (RuntimeException error) {
                handleResponse(follower.id(), request, null, error);
            }
        }
    }

    private void handleResponse(int followerId, AppendEntries request,
                                AppendEntriesResponse response, Throwable error) {
        synchronized (raftNode) {
            boolean isPendingRequest = pendingRequests.get(followerId) == request;
            if (isPendingRequest) {
                pendingRequests.remove(followerId);
            }

            long currentTerm = raftNode.getCurrentTerm().get();
            if (response != null && response.term() > currentTerm) {
                raftNode.becomeFollower(response.term());
                raftNode.resetElectionTimer();
                return;
            }

            if (!isPendingRequest || error != null || response == null
                    || raftNode.getStatus() != NodeStatus.LEADER
                    || request.term() != currentTerm || response.term() != currentTerm) {
                return;
            }

            if (response.success()) {
                int matchIndex = request.prevLogIndex() + request.entries().size();
                int previousMatchIndex = raftNode.getMatchIndex().get(followerId);

                raftNode.getMatchIndex().put(followerId, matchIndex);
                raftNode.getNextIndex().put(followerId, matchIndex + 1);

                if (isRecovering(followerId)) {
                    recoveringFollowers.remove(followerId);
                    log.info("[Node {}]: Follower {} recovery completed: matchIndex={}, term={}",
                            raftNode.getConfig().getNodeId(), followerId, matchIndex, currentTerm);
                } else if (matchIndex > previousMatchIndex) {
                    log.info("[Node {}]: Replication confirmed by follower {}: firstIndex={}, lastIndex={}, term={}",
                            raftNode.getConfig().getNodeId(), followerId, previousMatchIndex + 1, matchIndex, currentTerm);
                }
                advanceCommitIndex();

            } else {
                int matchIndex = raftNode.getMatchIndex().get(followerId);
                if (isRecovering(followerId)) {
                    return;
                }
                if (matchIndex > 0 && request.prevLogIndex() == matchIndex) {
                    recoveringFollowers.put(followerId, currentTerm);
                    log.warn("[Node {}]: Follower {} rejected confirmed index {}; starting full log recovery in term {}",
                            raftNode.getConfig().getNodeId(), followerId, matchIndex, currentTerm);
                    return;
                }

                int nextIndex = request.prevLogIndex();
                if (response.conflictIndex() != null) {
                    nextIndex = Math.min(nextIndex, response.conflictIndex());
                }
                raftNode.getNextIndex().put(followerId, Math.max(matchIndex + 1, nextIndex));
            }
        }
    }

    private void advanceCommitIndex() {
        List<NodeInfo> nodes = raftNode.getClusterProperties().getNodes();
        int majority = nodes.size() / 2 + 1;
        long currentTerm = raftNode.getCurrentTerm().get();

        for (int index = raftNode.getLastLogIndex(); index > raftNode.getCommitIndex(); index--) {
            if (raftNode.getLogTerm(index) != currentTerm) {
                break;
            }

            int replicatedCount = 1;
            for (NodeInfo node : nodes) {
                if (node.id() != raftNode.getConfig().getNodeId() && !isRecovering(node.id())
                        && raftNode.getMatchIndex().getOrDefault(node.id(), 0) >= index) {
                    replicatedCount++;
                }
            }

            if (replicatedCount >= majority) {
                raftNode.setCommitIndex(index);
                log.info("[Node {}]: Committed log through index={}, term={}",
                        raftNode.getConfig().getNodeId(), index, currentTerm);
                break;
            }
        }
    }

    private boolean isRecovering(int followerId) {
        Long recoveryTerm = recoveringFollowers.get(followerId);
        return recoveryTerm != null && recoveryTerm == raftNode.getCurrentTerm().get();
    }
}
