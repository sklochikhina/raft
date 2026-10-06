package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.AppendEntries;
import ru.nsu.klochikhina.raft.dto.AppendEntriesResponse;
import ru.nsu.klochikhina.raft.dto.RequestVote;
import ru.nsu.klochikhina.raft.dto.RequestVoteResponse;
import ru.nsu.klochikhina.raft.model.NodeStatus;
import ru.nsu.klochikhina.raft.property.ClusterProperties;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class NetworkService {

    private final RestClient raftRestClient;
    private final RaftNode raftNode;
    private final ClusterProperties clusterProperties;

    private final String REQUEST_VOTE = "/raft/request_vote";
    private final String APPEND_ENTRIES = "/raft/append_entries";

    public void sendRequestVote(RequestVote requestVote) {
        var nodes = clusterProperties.getNodes();
        int totalNodes = nodes.size();
        AtomicInteger votedCount = new AtomicInteger(1);
        long electionTerm = requestVote.term();

        for (var node : nodes) {
            if (Objects.equals(raftNode.getConfig().getNodeId(), node.id())) {
                continue;
            }
            CompletableFuture.supplyAsync(() -> {
                try {
                    return raftRestClient.post()
                            .uri("http://" + node.host() + ":" + node.port() + REQUEST_VOTE)
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(requestVote)
                            .retrieve()
                            .onStatus(HttpStatusCode::isError, (_, _) -> {
                                throw new RestClientException("Could not send a vote request to node: " + node.host() + ":" + node.port());
                            })
                            .body(RequestVoteResponse.class);
                } catch (Exception e) {
                    log.debug("[Raft] Failed to get vote from node {}: {}", node.id(), e.getMessage());
                    return null;
                }
            }).thenAccept(requestVoteResponse -> {
                if (requestVoteResponse == null) return;

                if (requestVoteResponse.term() > raftNode.getCurrentTerm().get()) {
                    raftNode.becomeFollower(requestVoteResponse.term());
                    raftNode.resetElectionTimer();
                } else if (requestVoteResponse.voteGranted() && raftNode.getStatus() == NodeStatus.CANDIDATE && raftNode.getCurrentTerm().get() == electionTerm) {
                    if (votedCount.incrementAndGet() > totalNodes / 2) {
                        raftNode.becomeLeader();
                    }
                }
            });
        }
    }

    public void sendAppendEntries(AppendEntries appendEntries) {
        for (var node : clusterProperties.getNodes()) {
            if (Objects.equals(raftNode.getConfig().getNodeId(), node.id())) {
                continue;
            }
            CompletableFuture.supplyAsync(() -> {
                try {
                    return raftRestClient.post()
                            .uri("http://" + node.host() + ":" + node.port() + APPEND_ENTRIES)
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(appendEntries)
                            .retrieve()
                            .onStatus(HttpStatusCode::isError, (_, _) -> {
                                throw new RestClientException("Could not send an append entries request to node: " + node.host() + ":" + node.port());
                            })
                            .body(AppendEntriesResponse.class);
                } catch (Exception e) {
                    return null;
                }
            }).thenAccept(response -> {
                if (response != null && response.term() > raftNode.getCurrentTerm().get()) {
                    raftNode.becomeFollower(response.term());
                    raftNode.resetElectionTimer();
                }
            });
        }
    }
}
