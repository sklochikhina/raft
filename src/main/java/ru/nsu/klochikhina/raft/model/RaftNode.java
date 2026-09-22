package ru.nsu.klochikhina.raft.model;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.nsu.klochikhina.raft.dto.NodeInfo;

@Component
@Getter
@Slf4j
@RequiredArgsConstructor
public class RaftNode {

    private final NodeConfig config;

    private NodeStatus status;
    private Integer currentTerm;
    private Integer votedFor;

    @PostConstruct
    public void init() {
        this.status = NodeStatus.FOLLOWER;
        this.currentTerm = 0;
        this.votedFor = null;
        this.logInfo();
    }

    public void becomeCandidate() {
        this.status = NodeStatus.CANDIDATE;
        this.currentTerm++;
        this.votedFor = this.config.getNodeId();
    }

    public void becomeFollower(int term, int votedFor) {
        this.status = NodeStatus.FOLLOWER;
        this.currentTerm = term;
        this.votedFor = votedFor;
    }

    public void becomeLeader() {
        this.status = NodeStatus.LEADER;
    }

    public void logInfo() {
        log.info("[Node {}]: port = {}, status = {}, currentTerm = {}", config.getNodeId(), config.getPort(), status, currentTerm);
    }

    public NodeInfo nodeInfo() {
        return new NodeInfo(config.getNodeId(), config.getPort(), status, currentTerm);
    }
}
