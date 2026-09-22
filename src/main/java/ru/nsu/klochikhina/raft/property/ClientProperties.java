package ru.nsu.klochikhina.raft.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "client")
@Data
public class ClientProperties {
    private int connectTimeout;
    private int readTimeout;
}
