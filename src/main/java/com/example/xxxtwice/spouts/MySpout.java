package com.example.xxxtwice.spouts;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;
import java.util.Objects;

/**
 * @Titile :  MySpouts
 * @Author :  ruthlessHardt
 */
public class MySpout extends BaseRichSpout {
    Map map;
    TopologyContext context;
    SpoutOutputCollector collector;
    private BufferedReader mReader;
    String log = null;

    /**
     * Configure initialization Spout class to enhance the scope
     * */
    @Override
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        // TODO Auto-generated method stub
        //Get local log file data
        InputStream in = this.getClass().getClassLoader().getResourceAsStream("\\static\\logdata\\info.log");
        mReader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(in)));
        //Write data to use down
        this.map = map;
        this.context = context;
        this.collector = collector;
    }

    /**
     * Collect, send data backwards
     * */
    @Override
    public void nextTuple() {
        try {
            while (null != (log = mReader.readLine())) {
                String[] split = log.split(",");
                if(split.length > 0 && split[3]!=null && "文具类用品".equals(split[3])){
                    this.collector.emit(new Values(log));
//                    Utils.sleep(1000);
                    System.err.println(" The data written is： ---------> "+ log );
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * The field name of the data to send data to the logical processing unit of the receiving data
     * */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));
    }

}
