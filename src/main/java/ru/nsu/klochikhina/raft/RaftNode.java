package ru.nsu.klochikhina.raft;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.nsu.klochikhina.raft.model.NodeConfig;
import ru.nsu.klochikhina.raft.model.NodeStatus;
import ru.nsu.klochikhina.raft.property.ClusterProperties;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Data
@Slf4j
@RequiredArgsConstructor
public class RaftNode {

    private final NodeConfig config;
    private final ClusterProperties clusterProperties;

    private volatile NodeStatus status;
    private final AtomicLong currentTerm = new AtomicLong(0L);
    private volatile Integer votedFor;

    private volatile long lastHeartbeat;
    private volatile long electionTimeout;

    private final long lowerLimit = 250L;
    private final long upperLimit = 451L;

    @PostConstruct
    public void init() {
        status = NodeStatus.FOLLOWER;
        currentTerm.set(0L);
        votedFor = null;
        lastHeartbeat = System.currentTimeMillis();
        electionTimeout = randomElectionTimeout();

        logNodeInfo();
        logKnownNodes();
    }

    public synchronized void becomeCandidate() {
        status = NodeStatus.CANDIDATE;
        long newTerm = currentTerm.incrementAndGet();
        votedFor = config.getNodeId();
        log.info("[Node {}]: Became CANDIDATE for term {}", config.getNodeId(), newTerm);
    }

    public synchronized void becomeFollower(long term) {
        if (term > currentTerm.get() || status != NodeStatus.FOLLOWER) {
            status = NodeStatus.FOLLOWER;
            currentTerm.set(term);
            votedFor = null;
            log.info("[Node {}]: Became FOLLOWER for term {}", config.getNodeId(), term);
        }
    }

    public synchronized void becomeLeader() {
        if (status != NodeStatus.CANDIDATE) {
            return;
        }
        status = NodeStatus.LEADER;
        log.info("[Node {}]: Became LEADER for term {}", config.getNodeId(), currentTerm.get());
    }

    private long randomElectionTimeout() {
        return ThreadLocalRandom.current().nextLong(lowerLimit, upperLimit);
    }

    public void resetElectionTimer() {
        lastHeartbeat = System.currentTimeMillis();
        electionTimeout = randomElectionTimeout();
    }

    private void logNodeInfo() {
        log.info("[Node {}]: port = {}, status = {}, term = {}", config.getNodeId(), config.getPort(), status, currentTerm);
    }

    private void logKnownNodes() {
        log.info("[Node {}]: known nodes = {}", config.getNodeId(), clusterProperties.getNodes());
    }
}
