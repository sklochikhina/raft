package ru.nsu.klochikhina.raft.model;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Getter
public class NodeConfig {

    @Value("${node.id}")
    private int nodeId;

    @Value("${server.port}")
    private int port;
}
