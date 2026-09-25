package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.AppendEntries;
import ru.nsu.klochikhina.raft.dto.AppendEntriesResponse;
import ru.nsu.klochikhina.raft.dto.RequestVote;
import ru.nsu.klochikhina.raft.dto.VoteResponse;
import ru.nsu.klochikhina.raft.property.ClusterProperties;

import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class NetworkService {

    private final RestClient raftRestClient;
    private final RaftNode raftNode;
    private final ClusterProperties clusterProperties;

    private final String voteUrl = "/raft/request-vote";
    private final String appendEntriesUrl = "/raft/append-entries";

    public void sendRequestVote(RequestVote requestVote) {
        log.info("[Raft] Requesting vote for newTerm {} from node {}.", raftNode.getCurrentTerm(), raftNode.getConfig().getNodeId());

        int votedCount = 1;
        for (var node : clusterProperties.getNodes()) {
            if (Objects.equals(raftNode.getConfig().getNodeId(), node.id())) {
                continue;
            }
            try {
                var voteResponse = raftRestClient.post()
                        .uri("http://" + node.host() + ":" + node.port() + voteUrl)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(requestVote)
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, (_, _) -> {
                            throw new RestClientException("Could not send a vote request to node: " + node.host() + ":" + node.port());
                        })
                        .body(VoteResponse.class);

                if (voteResponse.currentTerm() > raftNode.getCurrentTerm().get()) {
                    raftNode.becomeFollower(voteResponse.currentTerm());
                    log.info("[Raft] Node {} has a higher newTerm, becoming a follower...", node.id());
                    raftNode.resetElectionTimer();
                    return;

                } else if (voteResponse.voteGranted()) {
                    votedCount++;
                }

                log.info("[Raft] Vote response from node {}: vote granted? = {}.", node.id(), voteResponse.voteGranted());

            } catch (ResourceAccessException e) {
                logUnreachableNode(node.id());

            } catch (RestClientException e) {
                logRespondedWithError(node.id(), e.getMessage());

            } catch (Exception e) {
                logUnexpectedError(node.id(), e.getMessage());
            }
        }

        if (votedCount > clusterProperties.getNodes().size() / 2) {
            raftNode.becomeLeader();
            log.info("[Raft] Node {} has been chosen as a leader, new newTerm = {}.", raftNode.getConfig().getNodeId(), raftNode.getCurrentTerm());
        }
    }

    public void sendAppendEntries(AppendEntries appendEntries) {
        log.info("[Raft] Sending append entries from LEADER (newTerm = {}) node {}.", raftNode.getCurrentTerm().get(), raftNode.getConfig().getNodeId());

        for (var node : clusterProperties.getNodes()) {
            if (Objects.equals(raftNode.getConfig().getNodeId(), node.id())) {
                continue;
            }
            try {
                var appendEntriesResponse = raftRestClient.post()
                        .uri("http://" + node.host() + ":" + node.port() + appendEntriesUrl)
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(appendEntries)
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, (_, _) -> {
                            throw new RestClientException("Could not send an append entries request to node: " + node.host() + ":" + node.port());
                        })
                        .body(AppendEntriesResponse.class);

                if (appendEntriesResponse.currentTerm() > raftNode.getCurrentTerm().get()) {
                    raftNode.becomeFollower(appendEntriesResponse.currentTerm());
                    log.info("[Raft] I fell behind the other nodes (current newTerm = {}), becoming a follower...", appendEntriesResponse.currentTerm());
                    raftNode.resetElectionTimer();
                    return;
                }

            } catch (ResourceAccessException e) {
                logUnreachableNode(node.id());

            } catch (RestClientException e) {
                logRespondedWithError(node.id(), e.getMessage());

            } catch (Exception e) {
                logUnexpectedError(node.id(), e.getMessage());
            }
        }
    }

    private void logUnreachableNode(int nodeId) {
        log.error("[Raft] Node {} is unreachable (Timeout/Refused).", nodeId);
    }

    private void logRespondedWithError(int nodeId, String message) {
        log.error("[Raft] Node {} responded with an error: {}", nodeId, message);
    }

    private void logUnexpectedError(int nodeId, String message) {
        log.error("[Raft] An unexpected error occurred while trying to reach node {}: {}", nodeId, message);
    }
}
