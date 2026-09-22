package ru.nsu.klochikhina.raft;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;
import ru.nsu.klochikhina.raft.property.ClientProperties;
import ru.nsu.klochikhina.raft.property.ClusterProperties;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({ClusterProperties.class, ClientProperties.class})
public class RaftApplication {

    static void main(String[] args) {
        SpringApplication.run(RaftApplication.class, args);
    }
}
