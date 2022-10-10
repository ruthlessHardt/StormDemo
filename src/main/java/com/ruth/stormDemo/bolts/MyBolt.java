package com.ruth.stormDemo.bolts;

import com.ruth.stormDemo.domain.LogDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Titile :  SaveRedisBolt
 * @Author :  ruthlessHardt
 * @Desribtion  Accept the data sent by the previous Spout, and seek and accumulate
 */
@Slf4j
public class MyBolt extends BaseRichBolt {
    Map stormConf;
    TopologyContext context;
    OutputCollector collector;

    private List<LogDTO> logDTOList = new ArrayList<>(20);
    /**
     * Statistics data
     */
    private int count;
    /**
     * Together of the purchase of the purchase
     */
    private int purchaseTimeCount = 1;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.stormConf = stormConf;
        this.context = context;
        this.collector = collector;
    }

    /**
     * retrieve data
     * */
    @Override
    public void execute(Tuple input) {
        count++;
        String logData = input.getStringByField("log");
        log.info("logData :{}",logData);
        String[] split = logData.split(",");
        String productName = split[0];
        String purchaseNum = split[1];
        String productPrice = split[2];
        String productType = split[3];
        String purchaseTime = split[4];
        if("文具类用品".equals(productType)){
            //The same purchase time, the purchase quantity will be accumulated
            if(logDTOList.size() > 0){
                LogDTO lastLogDto = logDTOList.get(logDTOList.size() - 1);
                if(lastLogDto.getPurchaseTime().equals(purchaseTime)){
                    purchaseTimeCount ++;
                    lastLogDto.setPurchaseNum(lastLogDto.getPurchaseNum() + Integer.parseInt(purchaseNum));
                }
            }
            //save in List
            LogDTO logDTO = new LogDTO();
            logDTO.setProductName(productName);
            logDTO.setPurchaseNum(Integer.parseInt(purchaseNum));
            logDTO.setProductPrice(Double.parseDouble(productPrice));
            logDTO.setProductType(productType);
            logDTO.setPurchaseTime(purchaseTime);
            logDTOList.add(logDTO);
            //Filter the current purchase quantity
            Integer purchaseNumInRedis = logDTOList.stream().filter(e ->
                            e.getPurchaseTime().equals(purchaseTime))
                    .collect(Collectors.toList())
                    .get(0).getPurchaseNum();

            System.out.println();
            System.out.println("===========================================");
            System.out.println(purchaseTime+" : "+purchaseNumInRedis);
            System.out.println("===========================================");
            System.out.println();
            log.info("logData execute() --->  " +
                            " 商品名 :{} , 数量(累加的):{} , 单价:{} , 类别:{} , 时间:{}, count次数:{}, 时间分组个数:{}",
                    productName, purchaseNum, productPrice, productType, purchaseTime, count, purchaseTimeCount);
            //Output (purchase time, purchase quantity)
            collector.emit(new Values(purchaseTime,purchaseNumInRedis.toString()));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Send data to the back
        declarer.declare(new Fields("purchaseTime", "purchaseNumInRedis"));
    }
}


