package com.itsz.flink.kafka.boot.rtq;

import com.taifook.rtq.client.tcp.RTQDataPublishible;
import com.taifook.rtq.client.tcp.RTQResponse;
import org.springframework.kafka.core.KafkaTemplate;

public class RtqPublisher implements RTQDataPublishible {

    private KafkaTemplate<String, RTQResponse> kafkaTemplate;

    private String topic;

    public RtqPublisher(KafkaTemplate<String, RTQResponse> kafkaTemplate, String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }


    @Override
    public void publish(RTQResponse rtqResponse) {
        kafkaTemplate.send(topic, rtqResponse);
    }


    public KafkaTemplate<String, RTQResponse> getKafkaTemplate() {
        return kafkaTemplate;
    }

    public void setKafkaTemplate(KafkaTemplate<String, RTQResponse> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
