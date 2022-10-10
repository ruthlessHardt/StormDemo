package com.ruth.stormDemo;

import com.ruth.stormDemo.bolts.MyBolt;
import com.ruth.stormDemo.mapper.StoreRedisMapper;
import com.ruth.stormDemo.spouts.MySpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @Titile :  Test (executing program)
 * @Author :  ruthlessHardt
 */
public class Test {

    /**
     * Construct the topology structure and put it in the cluster to run
     * @param args
     */
    public static void main(String[] args) throws Exception {
        TopologyBuilder tb = new TopologyBuilder();
//        // zookeeper connection string
//        String zkConnString = "192.168.72.104:2181,192.168.72.105:2181,192.168.72.106:2181";
//
//        // Connect Broker
//        BrokerHosts hosts = new ZkHosts(zkConnString);
//        // kafka topic
//        String topicName = "data-log";
//        // Prepare spout configuration
//        SpoutConfig spoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
//        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//
//        // kafkaSpout
//        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
//
//        // Set selecting Spout
//        tb.setSpout("kafkaSpout", kafkaSpout);

        tb.setSpout("LogDataSpout", new MySpout());
        //Specify the distribution strategy through ShuffleGrouping and specify the distribution strategy. Parallelism 1
        tb.setBolt("LogDataCountBolt", new MyBolt(),1).shuffleGrouping("LogDataSpout");

        //Save in redis
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("127.0.0.1").setPort(6379).setPassword("123456").build();
        RedisStoreMapper storeMapper = new StoreRedisMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);
        tb.setBolt("StoreBolt", storeBolt, 1).shuffleGrouping("LogDataCountBolt");

        //Create a local Storm cluster
        LocalCluster lc = new LocalCluster();
        Config config = new Config();
        //Submit test to local clusters, topology operation name, assignment information, topology creation
        lc.submitTopology("LogDataTopology", config, tb.createTopology());
    }
}


