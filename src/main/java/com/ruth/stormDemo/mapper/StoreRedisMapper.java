package com.ruth.stormDemo.mapper;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

/**
 * @Titile :  StoreRedisMapper
 * Define the mapping relationship of data in Tuple and Redis
 * @Author :  ruthlessHardt
 */
public class StoreRedisMapper implements RedisStoreMapper {
    private RedisDataTypeDescription description;
    private final String hashKey = "logDataTrend";

    public StoreRedisMapper() {
        description = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("purchaseTime");
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return tuple.getStringByField("purchaseNumInRedis");
    }
}
