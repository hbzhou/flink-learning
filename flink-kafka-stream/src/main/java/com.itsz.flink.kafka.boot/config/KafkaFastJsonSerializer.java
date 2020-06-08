package com.itsz.flink.kafka.boot.config;

import com.alibaba.fastjson.JSON;
import com.taifook.rtq.client.tcp.RTQResponse;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaFastJsonSerializer  implements Serializer<RTQResponse> {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, RTQResponse response) {
        return JSON.toJSONBytes(response);
    }

    @Override
    public void close() {

    }
}
