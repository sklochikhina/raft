package ru.nsu.klochikhina.raft.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatusCode;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import ru.nsu.klochikhina.raft.dto.PingResponse;
import ru.nsu.klochikhina.raft.property.ClusterProperties;

@Service
@RequiredArgsConstructor
@Slf4j
public class NetworkService {

    private final RestClient raftRestClient;
    private final ClusterProperties clusterProperties;

    private final String pingUrl = "/raft/ping";

    @Scheduled(fixedDelayString = "${cluster.ping-interval}")
    public void sendPing() {
        for (var node : clusterProperties.getNodes()) {
            try {
                PingResponse pingResponse = raftRestClient.get()
                        .uri("http://" + node.host() + ":" + node.port() + pingUrl)
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, (_, _) -> {
                            throw new RestClientException("Could not send ping to node: " + node.host() + ":" + node.port());
                        })
                        .body(PingResponse.class);

                log.info("[Raft] Ping response from node {}: {}", node.id(), pingResponse.message());

            } catch (ResourceAccessException e) {
                log.error("[Raft] Node {} is unreachable (Timeout/Refused).", node.id());

            } catch (RestClientException e) {
                log.error("[Raft] Node {} responded with un error.", node.id());

            } catch (Exception e) {
                log.error("[Raft] Unexpected error during ping of node {}: {}", node.id(), e.getMessage());
            }
        }
    }
}
