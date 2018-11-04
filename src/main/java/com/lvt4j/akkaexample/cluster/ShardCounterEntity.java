package com.lvt4j.akkaexample.cluster;

import java.io.Serializable;

import akka.actor.AbstractActor;
import akka.cluster.Cluster;
import akka.cluster.sharding.ShardRegion;
import akka.cluster.sharding.ShardRegion.MessageExtractor;
import lombok.Data;

/**
 *
 * @author lichenxi on 2018年8月1日
 */
public class ShardCounterEntity extends AbstractActor {

    Cluster cluster = Cluster.get(context().system());
    
    String shardId;
    String id;
    int count;
    
    public ShardCounterEntity() {
        shardId = context().parent().path().name();
        id = self().path().name();
    }
    
    @Override
    public void preStart() throws Exception {
        super.preStart();
        System.out.println("ShardCounterEntity("+shardId+":"+id+") start");
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Integer.class, this::delta)
            .match(String.class, "print"::equals, this::print)
            .match(String.class, "beginPassivate"::equals, this::beginPassivate)
            .match(String.class, "endPassivate"::equals, this::endPassivate)
            .match(String.class, "handoff"::equals, this::handoff)
        .build();
    }
    
    private void delta(int delta) {
        int orig = count;
        count += delta;
        System.out.println("CounterEntity("+shardId+":"+id+") val "+orig+" add "+delta+" is "+count);
    }
    private void print(String msg) {
        System.out.println("CounterEntity("+shardId+":"+id+") is "+count);
    }
    private void beginPassivate(String msg) {
        System.out.println("CounterEntity("+shardId+":"+id+") begin passivate");
        context().parent().tell(new ShardRegion.Passivate("endPassivate"), self());
    }
    private void endPassivate(String msg) {
        System.out.println("CounterEntity("+shardId+":"+id+") end passivate "+count);
        context().stop(self());
    }
    private void handoff(String msg) throws Exception {
        System.out.println("CounterEntity("+shardId+":"+id+") handoff");
        context().stop(self());
    }
    
    @Override
    public void postStop() {
        System.out.println("CounterEntity("+shardId+":"+id+") stop");
    }
    
    @Data
    public static class Msg implements Serializable {
        private static final long serialVersionUID = 1L;
        public static MessageExtractor extractor = new MessageExtractor() {
            @Override
            public String shardId(Object obj) {
                return String.valueOf(Math.abs(entityId(obj).hashCode())%2);
            }
            
            @Override
            public Object entityMessage(Object obj) {
                if(!(obj instanceof Msg)) return obj;
                return ((Msg)obj).data;
            }
            
            @Override
            public String entityId(Object obj) {
                if(ShardRegion.StartEntity.class==obj.getClass())
                    return ((ShardRegion.StartEntity)obj).entityId();
                if(!(obj instanceof Msg))return null;
                return ((Msg)obj).id;
            }
        };
        
        public final String id;
        public final Object data;
        
    }
    
    
}
