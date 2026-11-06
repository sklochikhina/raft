package ru.nsu.klochikhina.raft.support;

import ru.nsu.klochikhina.raft.RaftNode;
import ru.nsu.klochikhina.raft.model.NodeConfig;
import ru.nsu.klochikhina.raft.model.NodeInfo;
import ru.nsu.klochikhina.raft.property.ClusterProperties;

import java.util.ArrayList;
import java.util.List;

public final class RaftTestFixture {

    private RaftTestFixture() {
    }

    public static RaftNode node(int clusterSize) {
        return node(1, clusterSize);
    }

    public static RaftNode node(int nodeId, int clusterSize) {
        NodeConfig config = new NodeConfig() {
            @Override
            public int getNodeId() {
                return nodeId;
            }

            @Override
            public int getPort() {
                return 8080 + nodeId;
            }
        };
        ClusterProperties cluster = new ClusterProperties();
        List<NodeInfo> nodes = new ArrayList<>();
        for (int id = 1; id <= clusterSize; id++) {
            nodes.add(new NodeInfo(id, 8080 + id, "localhost"));
        }
        cluster.setNodes(nodes);
        RaftNode node = new RaftNode(config, cluster);
        node.init();
        return node;
    }

    public static void electLeader(RaftNode node) {
        node.becomeCandidate();
        node.becomeLeader();
    }
}
