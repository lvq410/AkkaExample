package com.lvt4j.akkaexample.cluster;

import java.lang.reflect.Method;

import com.lvt4j.akkaexample.Utils;

import akka.actor.AbstractActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.ClusterDomainEvent;
import akka.cluster.ClusterEvent.LeaderChanged;
import akka.cluster.ClusterEvent.MemberExited;
import akka.cluster.ClusterEvent.MemberJoined;
import akka.cluster.ClusterEvent.MemberLeft;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.ReachabilityChanged;
import akka.cluster.ClusterEvent.RoleLeaderChanged;
import akka.cluster.ClusterEvent.SeenChanged;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.cluster.Member;

/**
 *
 * @author lichenxi on 2018年8月1日
 */
public class ClusterEvtListener extends AbstractActor {

    Cluster cluster = Cluster.get(getContext().getSystem());
    
    @Override
    public void preStart() throws Exception {
        super.preStart();
        cluster.subscribe(getSelf(), ClusterDomainEvent.class);
    }
    
    @Override
    public void postStop() {
      cluster.unsubscribe(getSelf());
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(ClusterDomainEvent.class, this::cde)
            .build();
    }
    public void cde(ClusterDomainEvent cde) throws Exception {
        String evt = cde.getClass().getSimpleName();
        String msg = evt + " ";
        Method method = Utils.method(getClass(), evt);
        if(method!=null)  msg += method.invoke(this, cde);
        System.out.println(msg);
    }
    public String MemberJoined(MemberJoined e) {
        return member(e.member());
    }
    public String MemberUp(MemberUp e) {
        return member(e.member());
    }
    public Object SeenChanged(SeenChanged e) {
        return e;
    }
    public String ReachabilityChanged(ReachabilityChanged e) {
        return "allUnreachable "+e.reachability().allUnreachable();
    }
    public String UnreachableMember(UnreachableMember e) {
        return member(e.member());
    }
    public Object LeaderChanged(LeaderChanged e) {
        if(e.getLeader()==null) return "";
        return e.getLeader().port().get();
    }
    public Object RoleLeaderChanged(RoleLeaderChanged e) {
        return e;
    }
    public Object MemberExited(MemberExited e) {
        return member(e.member());
    }
    public Object MemberLeft(MemberLeft e) {
        return member(e.member());
    }
    public Object MemberRemoved(MemberRemoved e) {
        return member(e.member());
    }
    
    private String member(Member member) {
        return member.address().port().get()+" "+member.status();
    }

}
