package ru.nsu.klochikhina.raft.statemachine;

public interface StateMachine {
    void apply(String command);
}
