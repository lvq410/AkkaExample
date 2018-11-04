package com.lvt4j.akkaexample.cluster;

import akka.actor.AbstractActor;

/**
 *
 * @author lichenxi on 2018年8月14日
 */
public class RemoteDeploy extends AbstractActor {

    String id = getSelf().path().name();
    
    @Override
    public void preStart() throws Exception {
        super.preStart();
        System.out.println("RemoteDeploy "+id+" start");
    }
    
    @Override
    public void postStop() throws Exception {
        super.postStop();
        System.out.println("RemoteDeploy "+id+" stop");
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .matchAny(this::job)
            .build();
    }
    private void job(Object msg) {
        System.out.println("RemoteDeploy receive: "+msg);
    }

}
