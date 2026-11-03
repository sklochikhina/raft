package ru.nsu.klochikhina.raft;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import ru.nsu.klochikhina.raft.service.NetworkService;

@SpringBootTest(properties = {"node.id=1", "server.port=8081"})
class RaftApplicationTests {

    @MockitoBean
    private NetworkService networkService;

    @Test
    void contextLoads() {
    }

}
