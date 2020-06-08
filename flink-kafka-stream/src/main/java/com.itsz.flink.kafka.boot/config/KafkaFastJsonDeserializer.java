package com.itsz.flink.kafka.boot.config;

import com.alibaba.fastjson.JSON;
import com.taifook.rtq.client.tcp.RTQResponse;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaFastJsonDeserializer implements Deserializer<RTQResponse> {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public RTQResponse deserialize(String s, byte[] bytes) {
        return JSON.parseObject(bytes, RTQResponse.class);
    }

    @Override
    public void close() {

    }
}
