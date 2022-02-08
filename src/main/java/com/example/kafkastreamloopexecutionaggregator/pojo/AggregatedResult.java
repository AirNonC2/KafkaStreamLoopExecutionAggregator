package com.example.kafkastreamloopexecutionaggregator.pojo;

import lombok.Data;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

@Data
public class AggregatedResult {



    public static enum Status{
        CONTINUE,
        SUCCESSFUL,
        TIMEOUT,
        REPETITION
    }

    private ConcurrentLinkedQueue<Object> payloads;
    private Status executionStatus;
    private volatile AtomicInteger counter;
    private String nextStepId;

    public AggregatedResult() {
        executionStatus = Status.CONTINUE;
        counter = new AtomicInteger(0);
        payloads = new ConcurrentLinkedQueue<>();
    }

    public int incrementCounter(){
        return counter.incrementAndGet();
    }

    public void enqueuePayload(Object payload){
       this.payloads.add(payload);
    }


    public synchronized AggregatedResult timeOutExecution(Result event) {

        this.executionStatus = Status.TIMEOUT;

        return this;
    }

    public synchronized AggregatedResult completeExecution(Result event) {

        this.executionStatus = Status.SUCCESSFUL;

        return this;
    }

    public synchronized AggregatedResult alreadyFinalized(Result event) {
        this.executionStatus = Status.REPETITION;
        return this;
    }

    public synchronized Status getExecutionStatus() {
        return this.executionStatus;
    }

    public void setNextStepId(String nextStepId) {
        this.nextStepId = nextStepId;
    }

    public ConcurrentLinkedQueue<Object> getPayloads() {
        return payloads;
    }

    @Override
    public String toString(){
        return "" + this.executionStatus.toString() + " -- next step id -> " + this.nextStepId + "  counter -->" + this.counter;
    }
}
