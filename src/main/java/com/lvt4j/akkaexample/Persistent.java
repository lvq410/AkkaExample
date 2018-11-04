package com.lvt4j.akkaexample;

import java.io.File;
import java.math.BigDecimal;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Triple;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.PatternsCS;
import akka.persistence.AbstractPersistentActor;
import akka.persistence.DeleteMessagesSuccess;
import akka.persistence.DeleteSnapshotFailure;
import akka.persistence.DeleteSnapshotSuccess;
import akka.persistence.DeleteSnapshotsFailure;
import akka.persistence.DeleteSnapshotsSuccess;
import akka.persistence.RecoveryCompleted;
import akka.persistence.SaveSnapshotFailure;
import akka.persistence.SaveSnapshotSuccess;
import akka.persistence.SnapshotOffer;
import akka.persistence.SnapshotSelectionCriteria;

/**
 *
 * @author lichenxi on 2018年7月16日
 */
public class Persistent {

    static class PersistentActor extends AbstractPersistentActor {
        
        private BigDecimal amount = BigDecimal.ZERO;
        
        @Override
        public String persistenceId() {
            return "persistenceId";
        }
        @Override
        public Receive createReceiveRecover() {
            return receiveBuilder()
                .match(BigDecimal.class, this::recoverDelta)
                .match(SnapshotOffer.class, this::recoverSnapshot)
                .match(RecoveryCompleted.class, this::recoverCompleted)
                .build();
        }
        private void recoverDelta(BigDecimal delta) {
            System.out.println("recover by event : "+delta);
            amount = amount.add(delta);
        }
        private void recoverSnapshot(SnapshotOffer snapshotOffer) {
            System.out.println("recover by snapshot(seqNo:"+snapshotOffer.metadata().sequenceNr()+") : "+snapshotOffer.snapshot());
            this.amount = (BigDecimal) snapshotOffer.snapshot();
        }
        private void recoverCompleted(RecoveryCompleted recoveryCompleted) {
            System.out.println("recover completed");
        }
        
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(Triple.class, this::method)
                    .match(DeleteMessagesSuccess.class, this::delMsgSuc)
                    .match(SaveSnapshotSuccess.class, this::saveSnapSuc)
                    .match(SaveSnapshotFailure.class, this::saveSnapFail)
                    .match(DeleteSnapshotSuccess.class, this::delSnapSuc)
                    .match(DeleteSnapshotFailure.class, this::delSnapFail)
                    .match(DeleteSnapshotsSuccess.class, this::delSnapsSuc)
                    .match(DeleteSnapshotsFailure.class, this::delSnapsFail)
                    .build();
        }
        private void method(Triple<String, BigDecimal, BigDecimal> msg) throws Exception {
            Utils.method(getClass(), msg.getLeft()).invoke(this, msg);
        }
        public void print(Triple<String, BigDecimal, BigDecimal> msg) {
            System.out.println("amount "+amount+", lastSequenceNr "+lastSequenceNr());
        }
        public void delta(Triple<String, BigDecimal, BigDecimal> msg) {
            if(msg.getMiddle()==null) return;
            persist(msg.getMiddle(), delta->{
                amount = amount.add(delta);
            });
            if(msg.getRight()==null) return;
            persist(msg.getRight(), delta->{
                amount = amount.add(delta);
            });
        }
        public void lastestSeqNo(Triple<String, BigDecimal, BigDecimal> msg) {
            getSender().tell(lastSequenceNr(), getSelf());
        }
        public void delMsg(Triple<String, BigDecimal, BigDecimal> msg) {
            deleteMessages(msg.getMiddle().longValue());
        }
        public void delSnapshot(Triple<String, BigDecimal, BigDecimal> msg) {
            deleteSnapshot(msg.getMiddle().longValue());
        }
        public void delSnapshots(Triple<String, BigDecimal, BigDecimal> msg) {
            SnapshotSelectionCriteria criteria  = SnapshotSelectionCriteria.create(msg.getMiddle().longValue(), msg.getRight().longValue());
            deleteSnapshots(criteria);
        }
        public void delSnapshotsLastest(Triple<String, BigDecimal, BigDecimal> msg) {
            deleteSnapshots(SnapshotSelectionCriteria.latest());
        }
        public void snapshot(Triple<String, BigDecimal, BigDecimal> msg) {
            saveSnapshot(amount);
        }
        public void delMsgSuc(DeleteMessagesSuccess deleteMessagesSuccess) {
            System.out.println("del msg suc : "+deleteMessagesSuccess);
        }
        public void saveSnapSuc(SaveSnapshotSuccess saveSnapshotSuccess) {
            System.out.println("save snap suc : "+saveSnapshotSuccess);
        }
        public void saveSnapFail(SaveSnapshotFailure saveSnapshotFailure) {
            System.out.println("save snap fail : "+saveSnapshotFailure);
        }
        public void delSnapSuc(DeleteSnapshotSuccess deleteSnapshotSuccess) {
            System.out.println("del snap suc : "+deleteSnapshotSuccess);
        }
        public void delSnapFail(DeleteSnapshotFailure deleteSnapshotFailure) {
            System.out.println("del snap fail : "+deleteSnapshotFailure);
        }
        public void delSnapsSuc(DeleteSnapshotsSuccess deleteSnapshotsSuccess) {
            System.out.println("del snaps suc : "+deleteSnapshotsSuccess);
        }
        public void delSnapsFail(DeleteSnapshotsFailure deleteSnapshotsFailure) {
            System.out.println("del snaps fail : "+deleteSnapshotsFailure);
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
            .build();
        Config config = ConfigFactory.parseMap(configs);
        
        ActorSystem system = ActorSystem.create("sys", config);
        ActorRef persistentActor = system.actorOf(Props.create(PersistentActor.class), "persistentActor");
        
        System.out.println("//  brand new sys, amount 0, seqNo 0");
        persistentActor.tell(Triple.of("print", null, null), null);
        Thread.sleep(500);
        
        System.out.println("//  add 1, amount 1, seqNo 1");
        persistentActor.tell(Triple.of("delta", BigDecimal.ONE, null), null);
        persistentActor.tell(Triple.of("print", null, null), null);
        Thread.sleep(500);
        
        System.out.println("//  add (3,4), amount 8, seqNo 3, then snapshot");
        persistentActor.tell(Triple.of("delta", new BigDecimal(3), new BigDecimal(4)), null);
        persistentActor.tell(Triple.of("print", null, null), null);
        persistentActor.tell(Triple.of("snapshot", null, null), null);
        Thread.sleep(500);
        
        System.out.println("//  restart sys, recover use snapshot(8), amount 8, seqNo 3");
        system.terminate(); Thread.sleep(1000);
        system = ActorSystem.create("sys", config);
        persistentActor = system.actorOf(Props.create(PersistentActor.class), "persistentActor");
        persistentActor.tell(Triple.of("print", null, null), null);
        Thread.sleep(500);
        
        System.out.println("//  add (100,2), amount 110, seqNo 5");
        persistentActor.tell(Triple.of("delta", new BigDecimal(100), new BigDecimal(2)), null);
        persistentActor.tell(Triple.of("print", null, null), null);
        Thread.sleep(500);
        
        System.out.println("//  restart sys, recover use snapshot(8) and event[100, 2] , amount 110, seqNo 5");
        system.terminate(); Thread.sleep(1000);
        system = ActorSystem.create("sys", config);
        persistentActor = system.actorOf(Props.create(PersistentActor.class), "persistentActor");
        persistentActor.tell(Triple.of("print", null, null), null);
        Thread.sleep(500);
        
        System.out.println("//  del lastest msg, amount 110, seqNo 5");
        long seqNo = (long) PatternsCS.ask(persistentActor, Triple.of("lastestSeqNo", null, null), 1000).toCompletableFuture().get();
        persistentActor.tell(Triple.of("delMsg", new BigDecimal(seqNo), null), null); Thread.sleep(500);
        persistentActor.tell(Triple.of("print", null, null), null);
        Thread.sleep(500);
        
        System.out.println("//  add (5,6), amount 121, seqNo 7");
        persistentActor.tell(Triple.of("delta", new BigDecimal(5), new BigDecimal(6)), null);
        persistentActor.tell(Triple.of("print", null, null), null);
        Thread.sleep(500);
        
        System.out.println("//  restart sys, recover use snapshot(8) and event(5,6) , amount 19, seqNo 7");
        system.terminate(); Thread.sleep(1000);
        system = ActorSystem.create("sys", config);
        persistentActor = system.actorOf(Props.create(PersistentActor.class), "persistentActor");
        persistentActor.tell(Triple.of("print", null, null), null); Thread.sleep(500);
        
        System.out.println("//  del latest snapshot, amount 19, seqNo 7");
        persistentActor.tell(Triple.of("delSnapshotsLastest", null, null), null); Thread.sleep(500);
        persistentActor.tell(Triple.of("print", null, null), null);
        Thread.sleep(500);
        
        System.out.println("//  restart sys, recover use event(5,6) , amount 11, seqNo 7");
        system.terminate(); Thread.sleep(1000);
        system = ActorSystem.create("sys", config);
        persistentActor = system.actorOf(Props.create(PersistentActor.class), "persistentActor");
        persistentActor.tell(Triple.of("print", null, null), null); Thread.sleep(500);
        
        system.terminate();
    }
    
}
