package ru.nsu.klochikhina.raft.statemachine;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class StringStateMachine implements StateMachine {

    private final List<String> commands = new ArrayList<>();

    @Override
    public synchronized String apply(String command) {
        commands.add(command);
        return "applied: " + command;
    }

    public synchronized List<String> commands() {
        return List.copyOf(commands);
    }
}
