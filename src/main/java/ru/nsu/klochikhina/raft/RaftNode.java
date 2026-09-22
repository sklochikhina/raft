package ru.nsu.klochikhina.raft;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.nsu.klochikhina.raft.dto.NodeInfoResponse;
import ru.nsu.klochikhina.raft.model.NodeConfig;
import ru.nsu.klochikhina.raft.model.NodeStatus;
import ru.nsu.klochikhina.raft.property.ClusterProperties;

@Component
@Data
@Slf4j
@RequiredArgsConstructor
public class RaftNode {

    private final NodeConfig config;

    private NodeStatus status;
    private Integer currentTerm;
    private Integer votedFor;

    private final ClusterProperties properties;

    @PostConstruct
    public void init() {
        this.status = NodeStatus.FOLLOWER;
        this.currentTerm = 0;
        this.votedFor = null;
        this.logNodeInfo();
        this.logKnownNodes();
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

    private void logNodeInfo() {
        log.info("[Node {}]: port = {}, status = {}, currentTerm = {}", config.getNodeId(), config.getPort(), status, currentTerm);
    }

    private void logKnownNodes() {
        log.info("[Node {}]: known nodes = {}", config.getNodeId(), properties.getNodes());
    }

    public NodeInfoResponse nodeInfo() {
        return new NodeInfoResponse(config.getNodeId(), config.getPort(), status, currentTerm);
    }
}
