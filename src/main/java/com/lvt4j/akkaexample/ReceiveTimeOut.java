package com.lvt4j.akkaexample;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;

/**
 *
 * @author lichenxi on 2018年8月7日
 */
public class ReceiveTimeOut {

    static class ReceiveTimeOutActor extends AbstractActor {

        @Override
        public void preStart() throws Exception {
            super.preStart();
        }
        
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                .match(String.class, "startByJavaDuration"::equals, this::startByJavaDuration)
                .match(String.class, "startByScalaDuration"::equals, this::startByScalaDuration)
                .match(ReceiveTimeout.class, this::timeout)
                .match(String.class, "cancelByMethod"::equals, this::cancelByMethod)
                .match(String.class, "cancelBySet"::equals, this::cancelBySet)
                .build();
        }
        private void startByJavaDuration(String msg) {
            getContext().setReceiveTimeout(java.time.Duration.ofSeconds(1));
        }
        private void startByScalaDuration(String msg) {
            getContext().setReceiveTimeout(scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS));
        }
        
        private void cancelByMethod(String msg) {
            getContext().cancelReceiveTimeout();
        }
        private void cancelBySet(String msg) {
            getContext().setReceiveTimeout(scala.concurrent.duration.Duration.Undefined());
        }
        
        private void timeout(ReceiveTimeout timeout) {
            System.out.println("ReceiveTimeout at "+Utils.dateFormat());
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.empty();
        ActorSystem system = ActorSystem.create("system", config);
        ActorRef ref = system.actorOf(Props.create(ReceiveTimeOutActor.class), "ReceiveTimeOutActor");
        
        System.out.println("//Start by java duration, should receive");
        ref.tell("startByJavaDuration", null); Thread.sleep(3000);
        
        System.out.println("//Cancel by method, should not work, still receive");
        ref.tell("cancelByMethod", null); Thread.sleep(3000);
        
        System.out.println("//Cancel by set, should work, will not receive");
        ref.tell("cancelBySet", null); Thread.sleep(3000);
        
        System.out.println("//Start by scala duration, should receive");
        ref.tell("startByScalaDuration", null); Thread.sleep(3000);
        
        System.out.println("//Cancel by method, should not work, still receive");
        ref.tell("cancelByMethod", null); Thread.sleep(3000);
        
        System.out.println("//Cancel by set, should work, will not receive");
        ref.tell("cancelBySet", null); Thread.sleep(3000);
        
        system.terminate();
    }
    
}
