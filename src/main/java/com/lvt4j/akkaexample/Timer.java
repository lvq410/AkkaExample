package com.lvt4j.akkaexample;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.AbstractActor;
import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Kill;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.japi.pf.DeciderBuilder;
import scala.Option;
import scala.concurrent.duration.Duration;

/**
 *
 * @author lichenxi on 2018年8月6日
 */
public class Timer {

    static ActorRef timerActorRef;
    
    static class ParentActor extends AbstractActor {

        @Override
        public void preStart() throws Exception {
            super.preStart();
            timerActorRef = getContext().actorOf(Props.create(TimerActor.class));
            
        }
        
        @Override
        public SupervisorStrategy supervisorStrategy() {
            return new OneForOneStrategy(1, Duration.create(1, TimeUnit.SECONDS),
                DeciderBuilder.matchAny(e->SupervisorStrategy.restart()).build());
        }
        
        @Override
        public Receive createReceive() {
            return receiveBuilder().build();
        }
        
    }
    
    static class TimerActor extends AbstractActorWithTimers {
        
        @Override
        public void preStart() throws Exception {
            System.out.println("Timer preStart");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                .match(String.class, "singleTimer"::equals, this::singleTimer)
                .match(String.class, "periodTimer"::equals, this::periodTimer)
                .match(String.class, "cancelSingleTimer"::equals, this::cancelSingleTimer)
                .match(String.class, "cancelPeriodTimer"::equals, this::cancelPeriodTimer)
                .match(String.class, "cancelPeriodTimerAfterLongProcess"::equals, this::cancelPeriodTimerAfterLongProcess)
                .match(String.class, "cancelUnknownTimer"::equals, this::cancelUnknownTimer)
                .match(String.class, this::msg)
                .build();
        }
        private void singleTimer(String msg) {
            getTimers().startSingleTimer("singleTimer", "single msg", Duration.create(3, TimeUnit.SECONDS));
        }
        private void periodTimer(String msg) {
            getTimers().startPeriodicTimer("periodTimer", "period msg", Duration.create(1, TimeUnit.SECONDS));
        }
        
        private void cancelSingleTimer(String msg) {
            getTimers().cancel("singleTimer");
        }
        private void cancelPeriodTimer(String msg) {
            getTimers().cancel("periodTimer");
        }
        private void cancelPeriodTimerAfterLongProcess(String msg) throws Exception {
            System.out.println("Long process begin");
            Thread.sleep(5000);
            getTimers().cancel("periodTimer");
            System.out.println("Long process end");
        }
        private void cancelUnknownTimer(String msg) {
            getTimers().cancel("unknownTimer");
        }
        
        private void msg(String msg) {
            System.out.println("Msg : "+msg);
        }
        
        @Override
        public void preRestart(Throwable reason, Option<Object> message)
                throws Exception {
            System.out.println("Timer preRestart");
        }
        
        @Override
        public void postStop() throws Exception {
            super.postStop();
            System.out.println("Timer postStop");
        }
        
        
        @Override
        public void postRestart(Throwable reason) throws Exception {
            super.postRestart(reason);
            System.out.println("Timer postRestart");
        }
        
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.empty();
        ActorSystem system = ActorSystem.create("system", config);
        system.actorOf(Props.create(ParentActor.class), "ParentActor"); Thread.sleep(1000);
        
        System.out.println("//Start single timer, should recieive");
        timerActorRef.tell("singleTimer", null); Thread.sleep(5000);
        
        System.out.println("//Start single timer twice, will not recieive twice");
        timerActorRef.tell("singleTimer", null);
        timerActorRef.tell("singleTimer", null);
        Thread.sleep(5000);
        
        System.out.println("//Start period timer, should recieive");
        timerActorRef.tell("periodTimer", null); Thread.sleep(5000);
        
        System.out.println("//Cancel period timer, should not recieive");
        timerActorRef.tell("cancelPeriodTimer", null); Thread.sleep(5000);
        
        System.out.println("//Cancel single timer, should not effect");
        timerActorRef.tell("cancelSingleTimer", null); Thread.sleep(1000);
        
        System.out.println("//Start period timer and cancel it after a long process msg, should not recieive");
        timerActorRef.tell("periodTimer", null); Thread.sleep(3000);
        timerActorRef.tell("cancelPeriodTimerAfterLongProcess", null);
        Thread.sleep(8000);
        
        System.out.println("//Cancel unknown timer, should not effect");
        timerActorRef.tell("cancelUnknownTimer", null); Thread.sleep(1000);
        
        System.out.println("//Start single timer and cancel quickly, should not effect");
        timerActorRef.tell("singleTimer", null);
        timerActorRef.tell("cancelSingleTimer", null);
        Thread.sleep(11000);
        
        System.out.println("//Start period timer and restart actor,should not receive more");
        timerActorRef.tell("periodTimer", null);
        Thread.sleep(3000);
        timerActorRef.tell(Kill.getInstance(), null);
        Thread.sleep(11000);
        
        system.terminate();
    }
    
}
