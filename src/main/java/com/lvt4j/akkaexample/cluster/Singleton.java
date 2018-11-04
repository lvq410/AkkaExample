package com.lvt4j.akkaexample.cluster;

import akka.actor.AbstractActor;

/**
 *
 * @author lichenxi on 2018年8月1日
 */
public class Singleton extends AbstractActor {

    @Override
    public void preStart() throws Exception {
        super.preStart();
        System.out.println("Singleton start");
    }
    
    @Override
    public void postStop() {
        System.out.println("Singleton stop");
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(String.class, this::msg)
            .build();
    }
    private void msg(String msg) {
        System.out.println("Singleton receive msg : "+msg);
    }
}