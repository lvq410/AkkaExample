package com.lvt4j.akkaexample.cluster;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck;
import lombok.SneakyThrows;

/**
 *
 * @author lichenxi on 2018年8月1日
 */
public class Subscriber extends AbstractActor {

    String type;
    String topic;
    String group;

    public Subscriber(String type, String topic, String group) {
        this.type = type;
        this.topic = topic;
        this.group = group;
    }
    
    @Override
    public void preStart() throws Exception {
        super.preStart();
        ActorRef mediator;
        switch(type){
        case "publish":
            mediator = DistributedPubSub.get(getContext().getSystem()).mediator();
            mediator.tell(new DistributedPubSubMediator.Subscribe(topic, getSelf()), getSelf());
            break;
        case "publishWithGroup":
            mediator = DistributedPubSub.get(getContext().getSystem()).mediator();
            mediator.tell(new DistributedPubSubMediator.Subscribe(topic, group, getSelf()), getSelf());
            break;
        case "send":
            mediator = DistributedPubSub.get(getContext().getSystem()).mediator();
            mediator.tell(new DistributedPubSubMediator.Put(getSelf()), getSelf());
            break;
        case "custom":
            getContext().actorSelection("/user/pubsubMediator")
                .tell(new DistributedPubSubMediator.Subscribe(topic, getSelf()), getSelf());
            break;
        }
        System.out.println("Subscriber start "+type);
    }
    
    @SneakyThrows
    @Override
    public void postStop() {
        ActorRef mediator;
        switch(type){
        case "publish":
            mediator = DistributedPubSub.get(getContext().getSystem()).mediator();
            mediator.tell(new DistributedPubSubMediator.Unsubscribe(topic, getSelf()), getSelf());
            break;
        case "publishWithGroup":
            mediator = DistributedPubSub.get(getContext().getSystem()).mediator();
            mediator.tell(new DistributedPubSubMediator.Unsubscribe(topic, group, getSelf()), getSelf());
            break;
        default: break;
        }
        System.out.println("Subscriber stop "+type);
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(String.class, this::msg)
            .match(SubscribeAck.class, this::ackSub)
            .build();
    }
    private void msg(String msg) {
        System.out.println("Receive "+msg);
    }
    private void ackSub(SubscribeAck ack) {
        System.out.println("Ack sub at "+topic +" "+group);
    }
}