package com.lvt4j.akkaexample.cluster;

import akka.actor.AbstractActor;
import akka.remote.AssociationEvent;
import akka.remote.QuarantinedEvent;
import akka.remote.RemotingLifecycleEvent;
import akka.remote.RemotingListenEvent;
import akka.remote.ThisActorSystemQuarantinedEvent;

/**
 *
 * @author lichenxi on 2018年8月14日
 */
public class RemoteEvtListener extends AbstractActor {

    
    
    @Override
    public void preStart() throws Exception {
        super.preStart();
        getContext().getSystem().eventStream().subscribe(getSelf(), RemotingLifecycleEvent.class);
        getContext().getSystem().eventStream().subscribe(getSelf(), RemotingListenEvent.class);
        getContext().getSystem().eventStream().subscribe(getSelf(), ThisActorSystemQuarantinedEvent.class);
    }
    
    @Override
    public void postStop() throws Exception {
        super.postStop();
        getContext().getSystem().eventStream().unsubscribe(getSelf());
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(AssociationEvent.class, this::AssociationEvent)
            .match(QuarantinedEvent.class, this::QuarantinedEvent)
            .match(RemotingListenEvent.class, this::RemotingListenEvent)
            .match(ThisActorSystemQuarantinedEvent.class, this::ThisActorSystemQuarantinedEvent)
            .match(RemotingLifecycleEvent.class, this::evt)
            .build();
    }
    /**
     * AssociatedEvent
     * AssociationErrorEvent
     * DisassociatedEvent
     * @param de
     */
    private void AssociationEvent(AssociationEvent de) {
        System.out.println(de.getClass().getSimpleName()+" "+de.eventName()+" local"+(de.inbound()?"<-":"->")+de.remoteAddress().port().get());
    }
    private void QuarantinedEvent(QuarantinedEvent qe) {
        System.out.println("QuarantinedEvent: "+qe);
    }
    private void RemotingListenEvent(RemotingListenEvent rl) {
        System.out.println("RemotingListenEvent: "+rl);
    }
    private void ThisActorSystemQuarantinedEvent(ThisActorSystemQuarantinedEvent qe) {
        System.out.println("ThisActorSystemQuarantinedEvent: "+qe);
    }
    private void evt(RemotingLifecycleEvent evt) {
        System.out.println("RemotingLifecycleEvent: "+evt.getClass().getSimpleName()+" "+evt);
    }

}
