package ru.nsu.klochikhina.raft.statemachine;

public interface StateMachine {
    String apply(String command);
}
