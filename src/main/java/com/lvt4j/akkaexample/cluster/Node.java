package com.lvt4j.akkaexample.cluster;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.lvt4j.akkaexample.Utils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.Member;
import akka.cluster.ddata.DistributedData;
import akka.cluster.ddata.Flag;
import akka.cluster.ddata.GCounter;
import akka.cluster.ddata.GSet;
import akka.cluster.ddata.Key;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWRegister;
import akka.cluster.ddata.ORMap;
import akka.cluster.ddata.ORMultiMap;
import akka.cluster.ddata.ORSet;
import akka.cluster.ddata.PNCounter;
import akka.cluster.ddata.PNCounterMap;
import akka.cluster.ddata.Replicator;
import akka.cluster.ddata.ReplicatorSettings;
import akka.cluster.pubsub.DistributedPubSub;
import akka.cluster.pubsub.DistributedPubSubMediator;
import akka.cluster.pubsub.DistributedPubSubSettings;
import akka.cluster.sharding.ClusterSharding;
import akka.cluster.sharding.ClusterShardingSettings;
import akka.cluster.sharding.ShardCoordinator;
import akka.cluster.sharding.ShardRegion;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import akka.remote.RemoteScope;
import lombok.SneakyThrows;
import scala.Option;
import scala.concurrent.duration.FiniteDuration;

/**
 *
 * @author lichenxi on 2018年8月1日
 */
public class Node {

    public static final String host = "localhost";
    public static final String protocol = "akka";
    public static final String sysName = "sys";
    
    private static final String Topic = "my-topic";
    private static final String TopicWithGroup = "my-topic-group";
    
    public static ActorSystem sys;
    public static Cluster cluster;
    public static BufferedReader manualReader;
    
    public static ActorRef pubsubMediator;
    public static ActorRef singletonProxy;
    public static ActorRef shardRegion;
    public static ActorRef shardRegionRemEntity;
    public static ActorRef defReplicator;
    public static ActorRef customReplicator;
    
    public static ActorRef ddata;
    
    
    protected static void start(int port) throws Exception {
        System.setProperty("java.io.tmpdir", "tmp");
        List<Address> addrs = range(2550, 2552).mapToObj(p->new Address(protocol, sysName, host, p)).collect(toList());
        Map<String, Object> configVals = ImmutableMap.<String, Object>builder()
            .put("akka.actor.provider", "cluster")
            .put("akka.cluster.seed-node-timeout", "10s")
            .put("akka.cluster.retry-unsuccessful-join-after", "10s")
            .put("akka.cluster.shutdown-after-unsuccessful-join-seed-nodes", "50s")
            .put("akka.coordinated-shutdown.phases.cluster-sharding-shutdown-region.recover", "off")
            .put("akka.coordinated-shutdown.terminate-actor-system", "on")
//            .put("akka.remote.quarantine-after-silence", "10s")
//            .put("akka.remote.system-message-buffer-size", "10")
//            .put("akka.remote.netty.tcp.hostname", host)
//            .put("akka.remote.netty.tcp.port", port)
            .put("akka.remote.artery.enabled", true)
            .put("akka.remote.artery.transport", "tcp")
            .put("akka.remote.artery.canonical.hostname", host)
            .put("akka.remote.artery.canonical.port", port)
            .put("akka.remote.artery.bind.hostname", "0.0.0.0")
            .put("akka.remote.artery.bind.port", port)
            .put("akka.remote.artery.advanced.outbound-control-queue-size", 10)
//            .put("akka.cluster.seed-nodes", range(0, 3).mapToObj(i->protocol+"://"+sys+"@"+host+":"+(2550+i)).collect(toList()))
            .put("akka.cluster.min-nr-of-members", 2)
            .put("akka.cluster.distributed-data.durable.keys", Arrays.asList("*"))
            .put("akka.cluster.distributed-data.durable.lmdb.dir", "ddata_"+port)
            .build();
        Config config = ConfigFactory.parseMap(configVals);
        sys = ActorSystem.create(sysName, config);
        sys.registerOnTermination(()->{
            System.out.println("Callback sys terminate");
        });
        
        sys.actorOf(Props.create(RemoteEvtListener.class), "remoteEvtListener");
        
        DistributedPubSubSettings pubSubSettings = DistributedPubSubSettings.create(sys);
        Props pubsubProps = DistributedPubSubMediator.props(pubSubSettings);
        pubsubMediator = sys.actorOf(pubsubProps, "pubsubMediator");
        
        ClusterSingletonManagerSettings singletonManagerSettings = ClusterSingletonManagerSettings.create(sys)
            .withSingletonName("singleton")
            .withHandOverRetryInterval(FiniteDuration.create(1, TimeUnit.SECONDS))
            .withRemovalMargin(FiniteDuration.create(10, TimeUnit.SECONDS));
        Props singletonProps = Props.create(Singleton.class);
        Props managerProps = ClusterSingletonManager.props(singletonProps, "close msg", singletonManagerSettings);
        sys.actorOf(managerProps, "singletonManager");
        ClusterSingletonProxySettings proxySettings = ClusterSingletonProxySettings.create(sys);
        Props proxyProps = ClusterSingletonProxy.props("/user/singletonManager", proxySettings);
        singletonProxy = sys.actorOf(proxyProps, "singletonProxy");
        
        ClusterShardingSettings shardingSettings = ClusterShardingSettings.create(sys);
        Props shardingEntityProps = Props.create(ShardCounterEntity.class);
        shardRegion = ClusterSharding.get(sys).start("Counter", shardingEntityProps, shardingSettings,
                ShardCounterEntity.Msg.extractor, new ShardCoordinator.LeastShardAllocationStrategy(10, 3), "handoff");
        
        ClusterShardingSettings shardingSettingsRemEnt = ClusterShardingSettings.create(sys)
            .withRememberEntities(true);
        Props shardingEntityRemEntProps = Props.create(ShardCounterEntity.class);
        shardRegionRemEntity = ClusterSharding.get(sys).start("CounterRemEnt", shardingEntityRemEntProps, shardingSettingsRemEnt,
            ShardCounterEntity.Msg.extractor, new ShardCoordinator.LeastShardAllocationStrategy(10, 3), "handoff");
        
        defReplicator = DistributedData.get(sys).replicator();
        
        ReplicatorSettings replicatorSettings = ReplicatorSettings.apply(sys);
        Props replicatorProps = Replicator.props(replicatorSettings);
        customReplicator = sys.actorOf(replicatorProps, "CustomReplicator");
        
        cluster = Cluster.get(sys);
        sys.actorOf(Props.create(ClusterEvtListener.class), "clusterEvtListener");
        cluster.registerOnMemberUp(()->{
            System.out.println("Callback member up");
            sys.actorOf(Props.create(Subscriber.class, "publish", Topic, null), "topic-subscriber");
            String group = String.valueOf(port%2);
            sys.actorOf(Props.create(Subscriber.class, "publishWithGroup", TopicWithGroup, group), "topic-subscriber_"+group);
            sys.actorOf(Props.create(Subscriber.class, "send", null, null), "subscriber");
            sys.actorOf(Props.create(Subscriber.class, "custom", Topic, null), "custom_subscriber");
            ddata = sys.actorOf(Props.create(DData.class), "ddata");
        });
        cluster.registerOnMemberRemoved(()->{
            System.out.println("Callback member removed");
        });
        cluster.joinSeedNodes(addrs);
        manual();
    }
    
    private static void manual() throws Exception {
        System.out.println("Ready...");
        manualReader = new BufferedReader(new InputStreamReader(System.in));
        while(true){
            String command = readLine();
            Method method = Utils.method(Node.class, command);
            try{
                if(method!=null) method.invoke(null);
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }
    @SneakyThrows
    private static String readLine() {
        return manualReader.readLine();
    }
    public static void exit() {
        System.exit(0);
    }
    public static void remoteDeploy() {
        System.out.println("Remote deploy, type port");
        int port = Integer.parseInt(readLine());
        Address addr = new Address(protocol, sysName, host, port);
        
        Props props = Props.create(RemoteDeploy.class).withDeploy(new Deploy(new RemoteScope(addr)));
        ActorRef ref = sys.actorOf(props, "RemoteActor");
        System.out.println(ref);
        ref.tell("hello", null);
    }
    public static void remoteDeploy100() {
        System.out.println("Remote deploy, type port");
        int port = Integer.parseInt(readLine());
        Address addr = new Address(protocol, sysName, host, port);
        Props props = Props.create(RemoteDeploy.class).withDeploy(new Deploy(new RemoteScope(addr)));
        for(int i=0; i<100; i++) sys.actorOf(props, "RemoteActor"+i);
    }
    public static void remoteDeployStop() {
        sys.actorSelection("/user/RemoteActor*").tell(PoisonPill.getInstance(), null);
    }
    public static void clusterState() {
        System.out.println("Cluster state:");
        Address leader = cluster.state().getLeader();
        Integer leaderPort = leader!=null?(Integer)leader.port().get():null;
        Set<Integer> unreachs = cluster.state().getUnreachable().stream()
                .map(Member::address).map(Address::port).map(Option::get)
                .map(Integer.class::cast).collect(Collectors.toSet());
        Iterable<Member> members = cluster.state().getMembers();
        for(Member member : members){
            int port = (int) member.address().port().get();
            String memberMsg = port+" "+member.status();
            if(Objects.equals(leaderPort, port)) memberMsg+=" Leader";
            if(unreachs.contains(port)) memberMsg+=" Unreachable";
            System.out.println(memberMsg);
        }
    }
    public static void sysTerminate() {
        sys.terminate();
    }
    public static void clusterDownSelf() {
        cluster.down(cluster.selfAddress());
    }
    public static void clusterDownUnreachable() {
        cluster.state().getUnreachable().forEach(m->cluster.down(m.address()));
    }
    public static void pubTopic() {
        DistributedPubSub.get(sys).mediator().tell(new DistributedPubSubMediator.Publish(Topic, "hello"), null);
    }
    @SneakyThrows
    public static void pubGroup() {
        DistributedPubSub.get(sys).mediator().tell(new DistributedPubSubMediator.Publish(TopicWithGroup, "hello with group", true), null);
    }
    public static void pubSend() {
        DistributedPubSub.get(sys).mediator().tell(new DistributedPubSubMediator.Send("/user/subscriber", "hello by send"), null);
    }
    public static void pubCustom() {
        pubsubMediator.tell(new DistributedPubSubMediator.Publish(Topic, "hello by custom"), null);
    }
    public static void singletonHello() {
        singletonProxy.tell("hello", null);
    }
    public static void counter() throws Exception {
        System.out.println("counter mode, please type region:  0:def;1:rememberEntities");
        ActorRef shardRegion = ImmutableMap.of("0", Node.shardRegion, "1", shardRegionRemEntity).get(readLine());
        System.out.println("please type counterId");
        String counterId = readLine();
        System.out.println("please type action:  0:delta;1:print;2:beginPassivate;3:region shut down");
        String action = readLine();
        switch(action){
        case "0":
            System.out.println("please type delta");
            int delta = Integer.parseInt(readLine());
            shardRegion.tell(new ShardCounterEntity.Msg(counterId, delta), null);
            break;
        case "1":
            shardRegion.tell(new ShardCounterEntity.Msg(counterId, "print"), null);
            break;
        case "2":
            shardRegion.tell(new ShardCounterEntity.Msg(counterId, "beginPassivate"), null);
            break;
        case "3":
            shardRegion.tell(ShardRegion.gracefulShutdownInstance(), null);
            break;
        }
    }
    public static void ddata() throws Exception {
        System.out.println("ddata mode, please type action:  0:get;1:update;2:del;3:flushChange");
        String action = ImmutableMap.of("0", "get", "1", "update", "2", "del", "3", "flushChange").get(readLine());
        Validate.notNull(action);
        System.out.println("please type data key");
        Key<?> dataKey = DData.dataTypeMap.get(readLine()).left;
        Validate.notNull(dataKey);
        System.out.println("please type replicator:  0:def;1:custom");
        ActorRef replicator = ImmutableMap.of("0", defReplicator, "1", customReplicator).get(readLine());
        Validate.notNull(replicator);
        if("flushChange".equals(action)){
            replicator.tell(Replicator.flushChanges(), null);
            return;
        }
        System.out.println("please type consistency level:  local;from;majority;all");
        String consistencyLvlStr = readLine();
        Map<String, Callable<Object>> readConsistencyLevel = ImmutableMap.of(
            "local", ()->Replicator.readLocal(),
            "majority", ()->DData.readMajority,
            "all", ()->DData.readAll,
            "from",()->{
                System.out.println("please typ number");
                return new Replicator.ReadFrom(Integer.parseInt(readLine()), DData.duration);
            });
        Map<String, Callable<Object>> writeConsistencyLevel = ImmutableMap.of(
            "local", ()->Replicator.writeLocal(),
            "majority", ()->DData.writeMajority,
            "all", ()->DData.writeAll,
            "from",()->{
                System.out.println("please type number");
                return new Replicator.WriteTo(Integer.parseInt(readLine()), DData.duration);
            });
        Object consistency = ("get".equals(action)?readConsistencyLevel:writeConsistencyLevel).get(consistencyLvlStr).call();
        Validate.notNull(consistency);
        if(Arrays.asList("get","del").contains(action)){
            ddata.tell(Pair.of(action, new Object[]{dataKey, replicator, consistency}), null);
            return;
        }
        Function<?, ?> modify = null; int delta;String ele;String type;String key;int val;
        switch(dataKey.id()){
        case "GCounter":
            System.out.println("please type increment number");
            delta = Integer.parseInt(readLine());
            modify = (Function<GCounter, GCounter>)a->a.increment(cluster, delta);
            break;
        case "PNCounter":
             System.out.println("please type increment/decrement number");
            delta = Integer.parseInt(readLine());
            if(delta>0) modify = (Function<PNCounter, PNCounter>)a->a.increment(cluster, delta);
            else modify = (Function<PNCounter, PNCounter>)a->a.decrement(cluster, Math.abs(delta));
            break;
        case "GSet":
            System.out.println("please type set ele");
            ele = readLine(); Validate.notBlank(ele);
            modify = (Function<GSet<String>, GSet<String>>)a->a.add(ele);
            break;
        case "ORSet":
            System.out.println("please type add/remove 0:add;1:remove");
            type = readLine(); Validate.notBlank(type);
            System.out.println("please type ele");
            ele = readLine(); Validate.notBlank(ele);
            if("add".equals(type)) modify = (Function<ORSet<String>, ORSet<String>>)a->a.add(cluster, ele);
            else modify = (Function<ORSet<String>, ORSet<String>>)a->a.remove(cluster, ele);
            break;
        case "ORMap":
            System.out.println("please type put/remove 0:put;1:remove");
            type = readLine(); Validate.notBlank(type);
            System.out.println("please type key");
            key = readLine(); Validate.notBlank(key);
            System.out.println("please type val");
            val = Integer.parseInt(readLine());
            if("0".equals(type)) modify = (Function<ORMap<String, GCounter>, ORMap<String, GCounter>>)a->a.put(cluster, key, GCounter.create().increment(cluster, val));
            else modify = (Function<ORMap<String, GCounter>, ORMap<String, GCounter>>)a->a.remove(cluster, key);
            break;
        case "ORMultiMap":
            System.out.println("please type put/remove 0:put;1:remove");
            type = readLine(); Validate.notBlank(type);
            System.out.println("please type key");
            key = readLine(); Validate.notBlank(key);
            System.out.println("please type val");
            val = Integer.parseInt(readLine());
            if("0".equals(type)) modify = (Function<ORMultiMap<String, Integer>, ORMultiMap<String, Integer>>)a->a.put(cluster, key, ImmutableSet.of(val));
            else modify = (Function<ORMultiMap<String, Integer>, ORMultiMap<String, Integer>>)a->a.remove(cluster, key);
            break;
        case "PNCounterMap":
            System.out.println("please type put/remove 0:put;1:remove");
            type = readLine(); Validate.notBlank(type);
            System.out.println("please type key");
            key = readLine(); Validate.notBlank(key);
            System.out.println("please type delta");
            delta = Integer.parseInt(readLine());
            if("0".equals(type))
                if(delta>0) modify = (Function<PNCounterMap<String>, PNCounterMap<String>>)a->a.increment(cluster, key, delta);
                else modify = (Function<PNCounterMap<String>, PNCounterMap<String>>)a->a.decrement(cluster, key, Math.abs(delta));
            else modify = (Function<PNCounterMap<String>, PNCounterMap<String>>)a->a.remove(cluster, key);
            break;
        case "LWWMap":
            System.out.println("please type put/remove 0:put;1:remove");
            type = readLine(); Validate.notBlank(type);
            System.out.println("please type key");
            key = readLine(); Validate.notBlank(key);
            System.out.println("please type val");
            val = Integer.parseInt(readLine());
            if("0".equals(type)) modify = (Function<LWWMap<String, Integer>, LWWMap<String, Integer>>)a->a.put(cluster, key, val);
            else modify = (Function<LWWMap<String, Integer>, LWWMap<String, Integer>>)a->a.remove(cluster, key);
            break;
        case "Flag":
            modify = (Function<Flag, Flag>)a->a.switchOn();
            break;
        case "LWWRegister":
            System.out.println("please type register ele");
            ele = readLine(); Validate.notBlank(ele);
            modify = (Function<LWWRegister<String>, LWWRegister<String>>)a->a.withValue(cluster, ele);
            break;
        }
        Validate.notNull(modify);
        ddata.tell(Pair.of("update", new Object[]{dataKey, replicator, consistency, modify}), null);
    }
}
