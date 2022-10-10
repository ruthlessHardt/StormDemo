package com.ruth.stormDemo.spouts;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @Titile :  KafkaSpout
 * @Author :  ruthlessHardt
 * @Date :  2022/8/7 10:35
 */
public class MyKafkaSpout extends BaseRichSpout {
    //数据收集器
    private SpoutOutputCollector collector;
    //Kafka消费者
    private KafkaConsumer<String, String> consumer;
    //Kafka消息主题
    private static final String TOPIC = "data-log";


    /**
     * 初始化操作
     *
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "storm.chris.com:9092");
        properties.put("group.id", "storm_group_01");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(new ArrayList<>(Collections.singleton(TOPIC)));
    }

    /**
     * 处理数据
     */
    @Override
    public void nextTuple() {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(10);
        if (null != consumerRecords) {
            consumerRecords.forEach(record ->
                    {
                        String[] split = record.value().split(",");
                        //筛选出文具类用品，并向后发送数据
                        if(split.length > 0 && split[3]!=null && "文具类用品".equals(split[3])){
                            collector.emit(new Values(record.value()));
                            Utils.sleep(1000);
                            System.err.println(" 写出的数据为： ---------> "+ record.value());
                        }
                    }
            );
        }
        Utils.sleep(1000);
    }

    /**
     * 定义输出流和字段
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //把数据存储在元组中名为line的字段里面
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
