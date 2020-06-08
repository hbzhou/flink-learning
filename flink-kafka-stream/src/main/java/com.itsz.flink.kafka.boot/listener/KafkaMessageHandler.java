package com.itsz.flink.kafka.boot.listener;

import com.alibaba.fastjson.JSONObject;
import com.itsz.flink.kafka.boot.rtq.RtqTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

//@Component
public class KafkaMessageHandler {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(topics = {RtqTopic.BORKER_QUEUE_TOPIC})
    public void listen(ConsumerRecord<?, ?> record) {
        logger.info("kafka的value: " + record.value().toString());
    }

    @KafkaListener(topics = {RtqTopic.RTQ_STREAM_TOPIC})
    public void listen2(ConsumerRecord<?, ?> record, Acknowledgment acknowledgment) {
        JSONObject jsonObject = JSONObject.parseObject(record.value().toString());
        logger.info("kafka2的value: {}", jsonObject.get("mappedResponse").toString());

    }

    @KafkaListener(topics = {RtqTopic.INDEX_TOPIC})
    public void listern3(ConsumerRecord record) {
        logger.info("kafka3的value: " + record.value().toString());
    }
}


