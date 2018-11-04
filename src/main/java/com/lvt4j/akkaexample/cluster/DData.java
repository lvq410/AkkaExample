package com.lvt4j.akkaexample.cluster;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.lvt4j.akkaexample.Utils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.cluster.Cluster;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.ReplicatedData;
import akka.cluster.ddata.Replicator;
import akka.cluster.ddata.Replicator.Changed;
import akka.cluster.ddata.Replicator.DataDeleted;
import akka.cluster.ddata.Replicator.Delete;
import akka.cluster.ddata.Replicator.DeleteSuccess;
import akka.cluster.ddata.Replicator.Get;
import akka.cluster.ddata.Replicator.GetFailure;
import akka.cluster.ddata.Replicator.GetSuccess;
import akka.cluster.ddata.Replicator.NotFound;
import akka.cluster.ddata.Replicator.ReadAll;
import akka.cluster.ddata.Replicator.ReadConsistency;
import akka.cluster.ddata.Replicator.ReadMajority;
import akka.cluster.ddata.Replicator.ReplicationDeleteFailure;
import akka.cluster.ddata.Replicator.StoreFailure;
import akka.cluster.ddata.Replicator.Update;
import akka.cluster.ddata.Replicator.UpdateFailure;
import akka.cluster.ddata.Replicator.UpdateSuccess;
import akka.cluster.ddata.Replicator.UpdateTimeout;
import akka.cluster.ddata.Replicator.WriteAll;
import akka.cluster.ddata.Replicator.WriteConsistency;
import akka.cluster.ddata.Replicator.WriteMajority;
import lombok.SneakyThrows;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

/**
 *
 * @author lichenxi on 2018年8月10日
 */
public class DData extends AbstractActor {

    Cluster node = Cluster.get(getContext().getSystem());
    
    public static FiniteDuration duration = Duration.create(1, TimeUnit.SECONDS);
    
    public static ReadMajority readMajority = new ReadMajority(duration);
    public static ReadAll readAll = new ReadAll(duration);
    public static WriteMajority writeMajority = new WriteMajority(duration);
    public static WriteAll writeAll = new WriteAll(duration);
    
    public static List<String> dataTypes = Arrays.asList("GCounter",
        "PNCounter","GSet","ORSet","ORMap","ORMultiMap",
        "PNCounterMap","LWWMap","Flag","LWWRegister");
    
    public static Map<String, ImmutablePair<Key<ReplicatedData>, ReplicatedData>> dataTypeMap = new HashMap<>();
    
    static{
        dataTypes.stream().forEach(Utils.consumer(t->{
            Class<?> keyCls = Class.forName("akka.cluster.ddata."+t+"Key");
            @SuppressWarnings("unchecked")
            Key<ReplicatedData> key = (Key<ReplicatedData>) Utils.method(keyCls, "create").invoke(null, t);
            Class<?> typeCls = Class.forName("akka.cluster.ddata."+t);
            ReplicatedData initialVal = (ReplicatedData) Utils.method(typeCls, "create").invoke(null);
            dataTypeMap.put(t, ImmutablePair.of(key, initialVal));
        }));
    }
    
    @Override
    public void preStart() throws Exception {
        super.preStart();
        dataTypeMap.values().forEach(v->Node.defReplicator.tell(new Replicator.Subscribe<>(v.left, getSelf()), getSelf()));
        dataTypeMap.values().forEach(v->Node.customReplicator.tell(new Replicator.Subscribe<>(v.left, getSelf()), getSelf()));
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(GetSuccess.class, this::getSuc)
            .match(NotFound.class, this::keyMsg)
            .match(GetFailure.class, this::keyMsg)
            .match(DataDeleted.class, this::keyMsg)
            .match(UpdateSuccess.class, this::keyMsg)
            .match(UpdateFailure.class, this::keyMsg)
            .match(StoreFailure.class, this::keyMsg)
            .match(UpdateTimeout.class, this::keyMsg)
            .match(DeleteSuccess.class, this::keyMsg)
            .match(ReplicationDeleteFailure.class, this::keyMsg)
            .match(ImmutablePair.class, p->"get".equals(p.left), this::get)
            .match(ImmutablePair.class, p->"del".equals(p.left), this::del)
            .match(ImmutablePair.class, p->"update".equals(p.left), this::update)
            .match(Changed.class, this::changed)
            .build();
    }
    private void getSuc(GetSuccess<?> msg) {
        System.out.println("GetSuccess : "+msg.key()+" : "+msg.dataValue());
    }
    @SneakyThrows
    private void keyMsg(Object msg) {
        System.out.println(msg.getClass().getSimpleName()+" : "+Utils.method(msg.getClass(), "key").invoke(msg));
    }
    private void get(Pair<String, Object[]> msg) {
        Object[] args = msg.getRight();
        Key<?> dataKey = (Key<?>) args[0];
        ActorRef replicator = (ActorRef) args[1];
        ReadConsistency consistency = (ReadConsistency) args[2];
        replicator.tell(new Get<>(dataKey, consistency), self());
    }
    private void del(Pair<String, Object[]> msg) {
        Object[] args = msg.getRight();
        Key<?> dataKey = (Key<?>) args[0];
        ActorRef replicator = (ActorRef) args[1];
        WriteConsistency consistency = (WriteConsistency) args[2];
        replicator.tell(new Delete<>(dataKey, consistency), self());
    }
    @SuppressWarnings("unchecked")
    private <T extends ReplicatedData> void update(Pair<String, Object[]> msg) {
        Object[] args = msg.getRight();
        Key<T> dataKey = (Key<T>) args[0];
        ActorRef replicator = (ActorRef) args[1];
        WriteConsistency consistency = (WriteConsistency) args[2];
        T initialVal = (T) dataTypeMap.get(dataKey.id()).right;
        Function<T, T> fun = (Function<T, T>) args[3];
        Update<T> update = new Update<T>(dataKey, initialVal, consistency, fun);
        replicator.tell(update, self());
    }
    private void changed(Changed<?> changed) {
        System.out.println("Changed :"+changed.key()+" "+changed.dataValue());
    }

}
