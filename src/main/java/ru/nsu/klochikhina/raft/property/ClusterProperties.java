package ru.nsu.klochikhina.raft.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import ru.nsu.klochikhina.raft.model.NodeInfo;

import java.util.List;

@ConfigurationProperties(prefix = "cluster")
@Data
public class ClusterProperties {
    private List<NodeInfo> nodes;
    private int pingInterval;
}
