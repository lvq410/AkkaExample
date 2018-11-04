package com.lvt4j.akkaexample;

import java.io.File;
import java.io.Serializable;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import com.google.common.collect.ImmutableMap;
import com.lvt4j.akkaexample.AtLeastOnce.ReceiverActor.SetBehavior;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.PatternsCS;
import akka.persistence.AbstractPersistentActorWithAtLeastOnceDelivery;
import akka.persistence.AtLeastOnceDelivery.AtLeastOnceDeliverySnapshot;
import akka.persistence.AtLeastOnceDelivery.UnconfirmedWarning;
import akka.persistence.RecoveryCompleted;
import akka.persistence.SaveSnapshotSuccess;
import akka.persistence.SnapshotOffer;
import lombok.Data;

/**
 *
 * @author lichenxi on 2018年7月25日
 */
public class AtLeastOnce {

    static class AtLeastOnceActor extends AbstractPersistentActorWithAtLeastOnceDelivery {

        ActorSelection receiverSelection = getContext().actorSelection("/user/receiver");
        
        String status;
        
        @Override
        public String persistenceId() {
            return "AtLeastOnceActor";
        }
        
        @Override
        public Receive createReceiveRecover() {
            return receiveBuilder()
                    .match(SnapshotOffer.class, this::recoverSnap)
                    .match(Triple.class, this::recoverMethod)
                    .match(RecoveryCompleted.class, this::recoverComplete)
                    .build();
        }
        public void recoverSnap(SnapshotOffer snapshotOffer) {
            @SuppressWarnings("unchecked")
            Pair<String, AtLeastOnceDeliverySnapshot> snapshot = (Pair<String, AtLeastOnceDeliverySnapshot>) snapshotOffer.snapshot();
            status = snapshot.getLeft();
            System.out.println("recover status by snapshot : "+status);
            setDeliverySnapshot(snapshot.getRight());
        }
        private void recoverMethod(Triple<String, String, Long> msg) throws Exception {
            Utils.method(getClass(), msg.getLeft()+"Recover").invoke(this, msg);
        }
        public void sendRecover(Triple<String, String, Long> msg) {
            System.out.println("recover send : "+msg);
            deliver(receiverSelection, deliverId->{
                System.out.println("recover deliver : "+msg.getMiddle()+" : "+deliverId);
                return Pair.of(deliverId, msg.getMiddle());
            });
        }
        public void confirmRecover(Triple<String, String, Long> msg) {
            System.out.println("recover confirm : "+msg);
            confirmDelivery(msg.getRight());
            status = msg.getMiddle();
        }
        public void recoverComplete(RecoveryCompleted recoveryCompleted) {
            System.out.println("recover complete");
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Triple.class, this::method)
                    .match(SaveSnapshotSuccess.class, this::saveSnapSuc)
                    .match(UnconfirmedWarning.class, this::unconfirm)
                    .build();
        }
        private void method(Triple<String, String, Long> msg) throws Exception {
            Utils.method(getClass(), msg.getLeft()).invoke(this, msg);
        }
        public void status(Triple<String, String, Long> msg) {
            getSender().tell(status, getSelf());
        }
        public void send(Triple<String, String, Long> msg) {
            System.out.println("persist send : "+msg);
            persist(msg, e->{
                deliver(receiverSelection, deliverId->{
                    System.out.println("deliver : "+msg.getMiddle()+" : "+deliverId);
                    return Pair.of(deliverId, msg.getMiddle());
                });
            });
        }
        public void confirm(Triple<String, String, Long> msg) {
            System.out.println("persist confirm : "+msg);
            persist(msg, e->{
                confirmDelivery(msg.getRight());
                System.out.println("confirm : "+msg);
                status = msg.getMiddle();
            });
        }
        public void snapshot(Triple<String, String, Long> msg) {
            AtLeastOnceDeliverySnapshot deliverySnapshot = getDeliverySnapshot();
            saveSnapshot(Pair.of(status, deliverySnapshot));
        }
        public void saveSnapSuc(SaveSnapshotSuccess saveSnapshotSuccess) {
            System.out.println("save snap suc : "+saveSnapshotSuccess);
        }
        public void unconfirm(UnconfirmedWarning unconfirmedWarning) {
            unconfirmedWarning.getUnconfirmedDeliveries().stream().forEach(ud->{
                System.out.println("unconfirm : "+ud.message()+" : "+ud.deliveryId());
            });
        }
        
    }
    
    static class ReceiverActor extends AbstractActor {

        Receive response = receiveBuilder()
            .match(SetBehavior.class, this::setBehavior)
            .match(Pair.class, this::response).build();
        Receive notResponse = receiveBuilder()
            .match(SetBehavior.class, this::setBehavior)
            .match(Pair.class, this::notResponse).build();
        
        Map<String, Receive> behaviorMap = ImmutableMap.of("response", response, "notResponse", notResponse);
        
        @Override
        public Receive createReceive() {
            return response;
        }
        public void setBehavior(SetBehavior setBehavior) {
            getContext().become(behaviorMap.getOrDefault(setBehavior.behaviorKey, response));
        }
        public void response(Pair<Long, String> msg) {
            System.out.println("receiver received and response : "+msg);
            getSender().tell(Triple.of("confirm", msg.getRight(), msg.getLeft()), getSelf());
        }
        public void notResponse(Pair<Long, String> msg) {
            System.out.println("receiver received and NOT response : "+msg);
        }
        
        @Data
        static class SetBehavior implements Serializable {
            private static final long serialVersionUID = 1L;
            public final String behaviorKey;
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        File journalFolder = new File("akka/persistence/journal"); journalFolder.mkdirs();
        File snapshotFolder = new File("akka/persistence/snapshots"); snapshotFolder.mkdirs();
        FileUtils.deleteDirectory(journalFolder);
        FileUtils.deleteDirectory(snapshotFolder);
        
        Map<String, Object> configs = ImmutableMap.<String, Object>builder()
            .put("akka.persistence.journal.plugin", "akka.persistence.journal.leveldb")
            .put("akka.persistence.snapshot-store.plugin", "akka.persistence.snapshot-store.local")
            .put("akka.persistence.journal.leveldb.dir", "akka/persistence/journal")
            .put("akka.persistence.snapshot-store.local.dir", "akka/persistence/snapshots")
            .put("akka.persistence.at-least-once-delivery.redeliver-interval", "1s")
            .put("akka.persistence.at-least-once-delivery.warn-after-number-of-unconfirmed-attempts", 3)
            .build();
        Config config = ConfigFactory.parseMap(configs);
        
        ActorSystem system = ActorSystem.create("sys", config);
        ActorRef senderRef = system.actorOf(Props.create(AtLeastOnceActor.class), "sender");
        ActorRef receiverRef = system.actorOf(Props.create(ReceiverActor.class), "receiver");
        
        System.out.println("//  brand new, send msg 'a', should confirm and status is 'a'");
        senderRef.tell(Triple.of("send", "a", null), null);
        Thread.sleep(1000);
        String status = (String) PatternsCS.ask(senderRef, Triple.of("status", null, null), 1000).toCompletableFuture().get();
        System.out.println("status : "+status);
        
        System.out.println("//  restart sys, should recover evts and not send and status is 'a'");
        system.terminate();
        system = ActorSystem.create("sys", config);
        senderRef = system.actorOf(Props.create(AtLeastOnceActor.class), "sender");
        receiverRef = system.actorOf(Props.create(ReceiverActor.class), "receiver");
        Thread.sleep(1000);
        status = (String) PatternsCS.ask(senderRef, Triple.of("status", null, null), 1000).toCompletableFuture().get();
        System.out.println("status : "+status);
        
        System.out.println("//  send msg 'b','c', should confirm and status is 'c'");
        senderRef.tell(Triple.of("send", "b", null), null);
        senderRef.tell(Triple.of("send", "c", null), null);
        Thread.sleep(1000);
        status = (String) PatternsCS.ask(senderRef, Triple.of("status", null, null), 1000).toCompletableFuture().get();
        System.out.println("status : "+status);
        
        System.out.println("//  set receiver not response, send msg 'd', should retry 3 times and unconfirm, during retry status is 'c'");
        receiverRef.tell(new SetBehavior("notResponse"), null);
        senderRef.tell(Triple.of("send", "d", null), null);
        Thread.sleep(1000);
        status = (String) PatternsCS.ask(senderRef, Triple.of("status", null, null), 1000).toCompletableFuture().get();
        System.out.println("status : "+status);
        Thread.sleep(3000);
        
        System.out.println("//  restart sys, should recover evts and resend&confirm msg 'd', and status is 'd'");
        system.terminate();
        system = ActorSystem.create("sys", config);
        senderRef = system.actorOf(Props.create(AtLeastOnceActor.class), "sender");
        receiverRef = system.actorOf(Props.create(ReceiverActor.class), "receiver");
        Thread.sleep(1000);
        status = (String) PatternsCS.ask(senderRef, Triple.of("status", null, null), 1000).toCompletableFuture().get();
        System.out.println("status : "+status);
        
        System.out.println("//  set receiver not response, send msg 'e','f'and save snapshot, should retry and during retry status is 'd'");
        System.out.println("//  then restart sys, should recover from snapshot and resend&confirm 'e','f', and status is 'f' ");
        receiverRef.tell(new SetBehavior("notResponse"), null);
        senderRef.tell(Triple.of("send", "e", null), null);
        senderRef.tell(Triple.of("send", "f", null), null);
        senderRef.tell(Triple.of("snapshot", null, null), null);
        Thread.sleep(2000);
        status = (String) PatternsCS.ask(senderRef, Triple.of("status", null, null), 1000).toCompletableFuture().get();
        System.out.println("status : "+status);
        system.terminate();
        system = ActorSystem.create("sys", config);
        senderRef = system.actorOf(Props.create(AtLeastOnceActor.class), "sender");
        receiverRef = system.actorOf(Props.create(ReceiverActor.class), "receiver");
        Thread.sleep(3000);
        status = (String) PatternsCS.ask(senderRef, Triple.of("status", null, null), 1000).toCompletableFuture().get();
        System.out.println("status : "+status);
        
        system.terminate();
    }
    
}
