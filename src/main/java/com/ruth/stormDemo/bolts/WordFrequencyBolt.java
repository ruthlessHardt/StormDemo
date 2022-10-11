package com.ruth.stormDemo.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WordFrequencyBolt extends BaseRichBolt {
    Map<String, Object> map;
    TopologyContext topologyContext;
    OutputCollector outputCollector;

    Map<String,Integer> objectMap = new HashMap<>(10);

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.map = map;
        this.topologyContext = topologyContext;
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("log");
        String[] split = word.split(",");
        String productName = split[0];
        Integer count = objectMap.get(productName);
        if(count == null){
            count = 0;
        }
        count++;
        objectMap.put(productName,count);
        System.out.println("-----------------------------------------------------");
        Set<Map.Entry<String,Integer>> set = objectMap.entrySet();
        for (Map.Entry<String, Integer> entry : set) {
            System.out.println(entry.getKey()+"-----------> " + entry.getValue());
        }
        outputCollector.emit(new Values(productName,count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Send data to the back
        outputFieldsDeclarer.declare(new Fields("words", "wordCount"));
    }
}
