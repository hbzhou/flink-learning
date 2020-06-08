package com.itsz.flink.kafka.boot.rtq;


import com.taifook.rtq.client.tcp.RTQCSConnector;
import com.taifook.rtq.client.tcp.RTQCSRequest;
import com.taifook.rtq.client.tcp.RTQResponse;
import com.taifook.rtq.util.RTQProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;


public class RtqConnector {

    private static final Logger logger = LoggerFactory.getLogger(RtqConnector.class);

    private final RTQProperties prop = new RTQProperties();

    private RTQCSConnector csConnector = null;

    private KafkaTemplate<String, RTQResponse> kafkaTemplate;

    private String topic;


    public RtqConnector(KafkaTemplate kafkaTemplate, String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;

    }

    public void connect() {
        try {
            prop.load(RtqConnector.class.getClassLoader().getResourceAsStream("rtqclient.properties"));
            this.csConnector = new RTQCSConnector(prop);
            this.csConnector.setPublishible(new RtqPublisher(this.kafkaTemplate,RtqTopic.topicMap.get(topic)));
            csConnector.connect();
            csConnector.logon();
            subscribe(topic);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public void subscribe(String topic) throws IOException {
        RTQCSRequest request = new RTQCSRequest();
        request.setRequest(topic);
        csConnector.subscribe(request);
    }

    public boolean isConnected() {
        return csConnector.isConnected();
    }

}
