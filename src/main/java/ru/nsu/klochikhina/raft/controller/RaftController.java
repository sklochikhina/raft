package ru.nsu.klochikhina.raft.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import ru.nsu.klochikhina.raft.dto.NodeInfo;
import ru.nsu.klochikhina.raft.service.RaftService;

@RequiredArgsConstructor
@RestController
public class RaftController {

    private final RaftService raftService;

    @GetMapping("/node/info")
    @ResponseBody
    public NodeInfo getNodeInfo() {
        return raftService.getNodeInfo();
    }
}
