package ru.nsu.klochikhina.raft.controller;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.model.LogEntry;
import ru.nsu.klochikhina.raft.model.NodeStatus;
import ru.nsu.klochikhina.raft.service.NetworkService;
import ru.nsu.klochikhina.raft.service.RaftService;
import ru.nsu.klochikhina.raft.service.ReplicationService;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static ru.nsu.klochikhina.raft.support.RaftTestFixture.*;

class RaftControllerTests {

    private RaftNode node;
    private NetworkService network;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        setUpCluster(3);
    }

    private void setUpCluster(int clusterSize) {
        node = node(clusterSize);
        electLeader(node);
        network = mock(NetworkService.class);
        when(network.sendAppendEntries(any(), any())).thenAnswer(_ -> new CompletableFuture<>());
        RaftService service = new RaftService(node, new ReplicationService(node, network));
        mvc = MockMvcBuilders.standaloneSetup(new RaftController(service)).build();
    }

    @Test
    void addEntriesReturnsActualAcceptedHttpStatusAndStartsReplication() throws Exception {
        mvc.perform(post("/raft/add_entries").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"entries\":[\"first\",\"second\"]}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("202 ACCEPTED"))
                .andExpect(jsonPath("$.committed").value(false));

        assertEquals(List.of(new LogEntry(1, "first"), new LogEntry(1, "second")), node.getLogEntries());
        verify(network, times(2)).sendAppendEntries(any(), any());
    }

    @Test
    void alreadyCommittedBatchReturnsOk() throws Exception {
        setUpCluster(1);

        mvc.perform(post("/raft/add_entries").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"entries\":[\"A\"]}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.committed").value(true));
    }

    @ParameterizedTest
    @EnumSource(value = NodeStatus.class, names = {"FOLLOWER", "CANDIDATE"})
    void nonLeaderReturnsConflict(NodeStatus role) throws Exception {
        node.setStatus(role);

        mvc.perform(post("/raft/add_entries").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"entries\":[\"A\"]}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.committed").value(false));
        assertTrue(node.getLogEntries().isEmpty());
        verifyNoInteractions(network);
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"entries\":null}", "{\"entries\":[]}",
            "{\"entries\":[\"A\",null]}", "{"})
    void invalidInputReturnsBadRequestWithoutChangingLog(String body) throws Exception {
        mvc.perform(post("/raft/add_entries").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());

        assertTrue(node.getLogEntries().isEmpty());
        verifyNoInteractions(network);
    }

    @Test
    void statusIncludesLogAndCommittedBoundaryOnFollower() throws Exception {
        node.setStatus(NodeStatus.FOLLOWER);
        node.getLogEntries().addAll(List.of(new LogEntry(1, "A"), new LogEntry(1, "B")));
        node.setCommitIndex(1);

        mvc.perform(get("/raft/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodeInfo.id").value(1))
                .andExpect(jsonPath("$.status").value("FOLLOWER"))
                .andExpect(jsonPath("$.currentTerm").value(1))
                .andExpect(jsonPath("$.peers.length()").value(2))
                .andExpect(jsonPath("$.entries.length()").value(2))
                .andExpect(jsonPath("$.entries[0].term").value(1))
                .andExpect(jsonPath("$.entries[1].command").value("B"))
                .andExpect(jsonPath("$.commitIndex").value(1));
    }

    @Test
    void removedShowLogEndpointReturnsNotFound() throws Exception {
        mvc.perform(get("/raft/show_log"))
                .andExpect(status().isNotFound());
    }

    @Test
    void statusEndpointReportsNodeAndPeers() throws Exception {
        mvc.perform(get("/raft/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.nodeInfo.id").value(1))
                .andExpect(jsonPath("$.status").value("LEADER"))
                .andExpect(jsonPath("$.currentTerm").value(1))
                .andExpect(jsonPath("$.peers.length()").value(2))
                .andExpect(jsonPath("$.entries").isEmpty())
                .andExpect(jsonPath("$.commitIndex").value(0));
    }

    @Test
    void requestVoteEndpointDeserializesLogMetadata() throws Exception {
        mvc.perform(post("/raft/request_vote").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"term\":2,\"candidateId\":2,\"lastLogTerm\":0,\"lastLogIndex\":0}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.voteGranted").value(true))
                .andExpect(jsonPath("$.term").value(2));
        assertEquals(2, node.getVotedFor());
    }

    @Test
    void appendEntriesEndpointStoresAndCommitsReplicatedCommands() throws Exception {
        mvc.perform(post("/raft/append_entries").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"term":2,"leaderId":2,"prevLogIndex":0,"prevLogTerm":0,
                                 "entries":[{"term":2,"command":"A"}],"leaderCommit":1}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.term").value(2));
        assertEquals(List.of(new LogEntry(2, "A")), node.getLogEntries());
        assertEquals(1, node.getCommitIndex());
    }
}
