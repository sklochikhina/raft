package ru.nsu.klochikhina.raft.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import ru.nsu.klochikhina.raft.property.ClientProperties;

@Configuration
@RequiredArgsConstructor
public class RaftConfig {

    private final ClientProperties clientProperties;

    @Bean
    public RestClient raftRestClient() {
        return RestClient.builder()
                .requestFactory(getRequestFactory())
                .build();
    }

    private ClientHttpRequestFactory getRequestFactory() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(clientProperties.getConnectTimeout());
        factory.setReadTimeout(clientProperties.getReadTimeout());
        return factory;
    }
}
