package com.lvt4j.akkaexample;

import static java.util.stream.Collectors.toList;

import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.AbstractActor;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.AllForOneStrategy;
import akka.actor.Identify;
import akka.actor.Inbox;
import akka.actor.OneForOneStrategy;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.Terminated;
import akka.japi.pf.DeciderBuilder;
import akka.pattern.PatternsCS;
import akka.routing.RandomGroup;
import akka.routing.RoundRobinGroup;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 *
 * @author lichenxi on 2018年7月23日
 */
public class Helloworld {

    @NoArgsConstructor
    @AllArgsConstructor
    static class Parent extends AbstractActor {
        
        String supervisorStrategyType = "";
        
        @Override
        public SupervisorStrategy supervisorStrategy() {
            switch(supervisorStrategyType){
            case "oneone_resume":
                return new OneForOneStrategy(DeciderBuilder.matchAny(o->SupervisorStrategy.resume()).build());
            case "allone_restart":
                return new AllForOneStrategy(DeciderBuilder.matchAny(o->SupervisorStrategy.restart()).build());
            default:
                return super.supervisorStrategy();
            }
        }
        
        @Override
        public void postRestart(Throwable reason) throws Exception {
            System.out.println("parent("+self()+") postRestart");
            super.postRestart(reason);
        }
        @Override
        public void preStart() throws Exception {
            System.out.println("parent("+self()+") preStart");
            super.preStart();
        }
        @Override
        public void postStop() throws Exception {
            System.out.println("parent("+self()+") postStop");
            super.postStop();
        }
        @Override
        public void preRestart(Throwable reason, Optional<Object> message)
                throws Exception {
            System.out.println("parent("+self()+") preRestart");
            super.preRestart(reason, message);
        }
        
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Pair.class, this::method)
                    .match(Terminated.class, this::terminated)
                    .build();
        }
        private void method(Pair<String, String> msg) throws Exception {
            Utils.method(getClass(), msg.getLeft()).invoke(this, msg);
        }
        public void print(Pair<String, String> msg) {
            System.out.println(msg.getRight());
        }
        public void stop(Pair<String, String> msg) {
            System.out.println(msg.getRight());
            getContext().stop(getSelf());
        }
        public void createChild(Pair<String, String> msg) {
            ActorRef childRef = getContext().actorOf(Props.create(Child.class), msg.getRight());
            getSender().tell(childRef, getSelf());
            getContext().watch(childRef);
        }
        public void terminated(Terminated terminated) {
            System.out.println("parent receive terminated:"+terminated);
        }
        
    }
    
    static class Child extends AbstractActor {
        @Override
        public void postRestart(Throwable reason) throws Exception {
            System.out.println("child("+self()+") postRestart");
            super.postRestart(reason);
        }
        @Override
        public void preStart() throws Exception {
            System.out.println("child("+self()+") preStart");
            super.preStart();
        }
        @Override
        public void postStop() throws Exception {
            System.out.println("child("+self()+") postStop");
            super.postStop();
        }
        @Override
        public void preRestart(Throwable reason, Optional<Object> message)
                throws Exception {
            System.out.println("child("+self()+") preRestart");
            super.preRestart(reason, message);
        }
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Pair.class, this::method)
                    .build();
        }
        private void method(Pair<String, String> msg) throws Exception {
            Utils.method(getClass(), msg.getLeft()).invoke(this, msg);
        }
        public void print(Pair<String, String> msg) {
            System.out.println(msg);
        }
        public void crash(Pair<String, String> msg) throws Exception {
            System.out.println("receive crash:"+getSelf()+":"+msg);
            throw new Exception(msg.getRight());
        }
        
    }

    public static void main(String[] args) throws Exception {
        Config config = ConfigFactory.empty();
        ActorSystem system = ActorSystem.create("system", config);
        ActorRef pRef = system.actorOf(Props.create(Parent.class), "parent");
        
        System.out.println("//  actorRef actorPath actorAddress");
        System.out.println("actorRef："+pRef);
        System.out.println("actorPath："+pRef.path());
        System.out.println("actorAddress："+pRef.path().address());
        Thread.sleep(1000);
        
        System.out.println("//  send msg by tell");
        pRef.tell(Pair.of("print", "helloworld by tell"), null);
        Thread.sleep(1000);
        
        System.out.println("//  send msg by selection");
        ActorSelection selection = system.actorSelection("/user/**");
        selection.tell(Pair.of("print", "helloworld by selection"), null);
        Thread.sleep(1000);
        
        System.out.println("//  send msg by inbox");
        Inbox inbox = Inbox.create(system);
        inbox.send(pRef, Pair.of("print", "helloworld by inbox"));
        Thread.sleep(1000);
        
        System.out.println("//  send Identify should receive actorRef");
        inbox.send(pRef, new Identify("identifyParent"));
        ActorIdentity actorIdentity = (ActorIdentity) inbox.receive(Duration.ofSeconds(1));
        pRef = actorIdentity.getActorRef().get();
        System.out.println("actorIdnetity："+actorIdentity.correlationId()+"："+pRef);
        
        System.out.println("//  send PoisonPill and send msg, should forward to deadletter");
        inbox.send(pRef, PoisonPill.getInstance());
        pRef.tell(Pair.of("print", "helloworld by deadletter"), null);
        Thread.sleep(1000);
        
        System.out.println("//  recreate parent actor, actorRef should not equal to previous");
        ActorRef pRef2 = system.actorOf(Props.create(Parent.class), "parent");
        System.out.println("pRef2："+pRef2);Thread.sleep(1000);
        pRef = pRef2;
        
        System.out.println("//  stop by context.stop and send msg, should forward to deadletter");
        pRef.tell(Pair.of("stop", "stop by tell"), null);
        pRef.tell(Pair.of("print", "helloworld by deadletter"), null);
        Thread.sleep(1000);
        
        System.out.println("//  create child0 and child1, use PatternsCS.ask to get return childRef");
        pRef = system.actorOf(Props.create(Parent.class), "parent");
        ActorRef child0Ref = (ActorRef) PatternsCS.ask(pRef, Pair.of("createChild", "child0"), 1000).toCompletableFuture().get();
        ActorRef child1Ref = (ActorRef) PatternsCS.ask(pRef, Pair.of("createChild", "child1"), 1000).toCompletableFuture().get();
        Thread.sleep(1000);
        
        System.out.println("//  child0 crash with default supervisorStrategy, one one restart");
        child0Ref.tell(Pair.of("crash", "child0 crash"), null);
        Thread.sleep(1000);
        
        System.out.println("//  recreate parent and child with supervisorStrategy--one one resume, and crash one child by round router");
        system.stop(pRef);Thread.sleep(1000);
        pRef = system.actorOf(Props.create(Parent.class, "oneone_resume"), "parent");
        child0Ref = (ActorRef) PatternsCS.ask(pRef, Pair.of("createChild", "child0"), 1000).toCompletableFuture().get();
        child1Ref = (ActorRef) PatternsCS.ask(pRef, Pair.of("createChild", "child1"), 1000).toCompletableFuture().get();
        ActorRef router = system.actorOf(new RoundRobinGroup(
            Arrays.asList(child0Ref, child1Ref).stream().map(ActorRef::path).map(Object::toString).collect(toList())).props(), "router");
        router.tell(Pair.of("crash", "child round crash"), null);
        system.stop(router);
        Thread.sleep(1000);
        
        System.out.println("//  recreate parent and child with supervisorStrategy--all one restart, and crash one child by rand router");
        system.stop(pRef);Thread.sleep(1000);
        pRef = system.actorOf(Props.create(Parent.class, "allone_restart"), "parent");
        child0Ref = (ActorRef) PatternsCS.ask(pRef, Pair.of("createChild", "child0"), 1000).toCompletableFuture().get();
        child1Ref = (ActorRef) PatternsCS.ask(pRef, Pair.of("createChild", "child1"), 1000).toCompletableFuture().get();
        router = system.actorOf(new RandomGroup(
            Arrays.asList(child0Ref, child1Ref).stream().map(ActorRef::path).map(Object::toString).collect(toList())).props(), "router");
        router.tell(Pair.of("crash", "child rand crash"), null);
        system.stop(router);
        Thread.sleep(1000);
        
        System.out.println("//  stop one child, and parent receive terminated msg");
        system.stop(pRef);Thread.sleep(1000);
        pRef = system.actorOf(Props.create(Parent.class), "parent");
        child0Ref = (ActorRef) PatternsCS.ask(pRef, Pair.of("createChild", "child0"), 1000).toCompletableFuture().get();
        child1Ref = (ActorRef) PatternsCS.ask(pRef, Pair.of("createChild", "child1"), 1000).toCompletableFuture().get();
        child1Ref.tell(PoisonPill.getInstance(), null);
        Thread.sleep(1000);
        
        System.out.println("//  sys terminate");
        Thread.sleep(1000);system.terminate();
    }
    
}
