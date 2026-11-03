package ru.nsu.klochikhina.raft.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.dto.AppendEntries;
import ru.nsu.klochikhina.raft.dto.AppendEntriesResponse;
import ru.nsu.klochikhina.raft.dto.RequestVote;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.model.NodeStatus;

import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;
import static ru.nsu.klochikhina.raft.support.RaftTestFixture.node;

class NetworkServiceTests {

    private RaftNode node;
    private NetworkService service;
    private MockRestServiceServer server;
    private RequestVote voteRequest;

    @BeforeEach
    void setUp() {
        setUpCluster(3);
    }

    private void setUpCluster(int size) {
        node = node(size);
        node.getLogEntries().addAll(List.of(new LogEntry(1, "A"), new LogEntry(2, "B")));
        node.getCurrentTerm().set(2);
        node.becomeCandidate();
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).ignoreExpectOrder(true).build();
        service = new NetworkService(builder.build(), node, node.getClusterProperties());
        voteRequest = new RequestVote(3, 1, 2, 2);
    }

    @AfterEach
    void verifyRequests() {
        server.verify();
    }

    @Test
    void sendsAppendEntriesOnlyToSpecifiedFollowerAndReturnsResponse() throws Exception {
        server.expect(requestTo("http://localhost:8082/raft/append_entries"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(content().json("""
                        {"term":3,"leaderId":1,"prevLogIndex":1,"prevLogTerm":1,
                         "entries":[{"term":2,"command":"B"}],"leaderCommit":1}
                        """))
                .andRespond(withSuccess("{\"success\":true,\"term\":3}", MediaType.APPLICATION_JSON));

        AppendEntriesResponse response = service.sendAppendEntries(node.getClusterProperties().getNodes().get(1),
                new AppendEntries(3, 1, 1, 1, List.of(new LogEntry(2, "B")), 1)).get(5, TimeUnit.SECONDS);

        assertEquals(new AppendEntriesResponse(true, 3), response);
        assertEquals(NodeStatus.CANDIDATE, node.getStatus());
    }

    @Test
    void appendEntriesTransportFailureReturnsMissingResponse() throws Exception {
        server.expect(requestTo("http://localhost:8082/raft/append_entries"))
                .andRespond(withException(new SocketTimeoutException("timeout")));

        assertNull(service.sendAppendEntries(node.getClusterProperties().getNodes().get(1),
                new AppendEntries(3, 1, 2, 2, List.of(), 0)).get(5, TimeUnit.SECONDS));
    }

    @Test
    void appendEntriesHttpErrorReturnsMissingResponse() throws Exception {
        server.expect(requestTo("http://localhost:8082/raft/append_entries"))
                .andRespond(withServerError());

        assertNull(service.sendAppendEntries(node.getClusterProperties().getNodes().get(1),
                new AppendEntries(3, 1, 2, 2, List.of(), 0)).get(5, TimeUnit.SECONDS));
    }

    @Test
    void ownVoteAndOnePeerElectLeaderInThreeNodeCluster() throws Exception {
        expectVote(2, "{\"voteGranted\":true,\"term\":3}");
        expectVote(3, "{\"voteGranted\":false,\"term\":3}");

        service.sendRequestVote(voteRequest).get(5, TimeUnit.SECONDS);

        assertEquals(NodeStatus.LEADER, node.getStatus());
        assertEquals(3, node.getNextIndex().get(2));
        assertEquals(0, node.getMatchIndex().get(2));
    }

    @Test
    void exactlyHalfOfVotesDoesNotElectLeader() throws Exception {
        setUpCluster(4);
        expectVote(2, "{\"voteGranted\":true,\"term\":3}");
        expectVote(3, "{\"voteGranted\":false,\"term\":3}");
        expectVote(4, "{\"voteGranted\":false,\"term\":3}");

        service.sendRequestVote(voteRequest).get(5, TimeUnit.SECONDS);

        assertEquals(NodeStatus.CANDIDATE, node.getStatus());
    }

    @Test
    void higherTermInVoteResponseMakesCandidateFollower() throws Exception {
        expectVote(2, "{\"voteGranted\":false,\"term\":4}");
        expectVote(3, "{\"voteGranted\":false,\"term\":3}");

        service.sendRequestVote(voteRequest).get(5, TimeUnit.SECONDS);

        assertEquals(NodeStatus.FOLLOWER, node.getStatus());
        assertEquals(4, node.getCurrentTerm().get());
        assertNull(node.getVotedFor());
    }

    @Test
    void responsesFromPreviousElectionCannotElectCurrentCandidate() throws Exception {
        for (int id : List.of(2, 3)) {
            server.expect(requestTo("http://localhost:" + (8080 + id) + "/raft/request_vote"))
                    .andRespond(request -> {
                        synchronized (node) {
                            if (node.getCurrentTerm().get() == 3) {
                                node.becomeCandidate();
                            }
                        }
                        return withSuccess("{\"voteGranted\":true,\"term\":3}", MediaType.APPLICATION_JSON)
                                .createResponse(request);
                    });
        }

        service.sendRequestVote(voteRequest).get(5, TimeUnit.SECONDS);

        assertEquals(4, node.getCurrentTerm().get());
        assertEquals(NodeStatus.CANDIDATE, node.getStatus());
    }

    @Test
    void responseWithOlderTermCannotCountAsGrantedVote() throws Exception {
        expectVote(2, "{\"voteGranted\":true,\"term\":2}");
        expectVote(3, "{\"voteGranted\":false,\"term\":3}");

        service.sendRequestVote(voteRequest).get(5, TimeUnit.SECONDS);

        assertEquals(NodeStatus.CANDIDATE, node.getStatus());
    }

    @Test
    void failedVoteRequestDoesNotCountAsVoteOrPreventAnotherResponse() throws Exception {
        server.expect(requestTo("http://localhost:8082/raft/request_vote")).andRespond(withServerError());
        expectVote(3, "{\"voteGranted\":false,\"term\":3}");

        service.sendRequestVote(voteRequest).get(5, TimeUnit.SECONDS);

        assertEquals(NodeStatus.CANDIDATE, node.getStatus());
        assertEquals(3, node.getCurrentTerm().get());
    }

    @Test
    void staleElectionIsNotSent() throws Exception {
        node.becomeFollower(4);

        service.sendRequestVote(voteRequest).get(5, TimeUnit.SECONDS);

        assertEquals(NodeStatus.FOLLOWER, node.getStatus());
        assertEquals(4, node.getCurrentTerm().get());
    }

    private void expectVote(int id, String response) {
        server.expect(requestTo("http://localhost:" + (8080 + id) + "/raft/request_vote"))
                .andExpect(method(HttpMethod.POST))
                .andExpect(content().json("{\"term\":3,\"candidateId\":1,\"lastLogTerm\":2,\"lastLogIndex\":2}"))
                .andRespond(withSuccess(response, MediaType.APPLICATION_JSON));
    }
}
