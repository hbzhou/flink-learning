package com.itsz.flink.kafka.boot.rtq;

import org.springframework.kafka.core.KafkaTemplate;

import java.util.ArrayList;
import java.util.List;

public class RtqConnectorManager {

    private List<RtqConnector> rtqConnectorList = new ArrayList<>();


    public void startup() {
        for (RtqConnector rtqConnector : rtqConnectorList) {
            rtqConnector.connect();
        }
    }

    public void addConnector(KafkaTemplate kafkaTemplate, String topic) {
        RtqConnector rtqConnector = new RtqConnector(kafkaTemplate, topic);
        this.rtqConnectorList.add(rtqConnector);
    }


    public boolean isConnected() {
        for (RtqConnector rtqConnector : rtqConnectorList) {
            if (!rtqConnector.isConnected()) {
                return false;
            }
        }
        return true;
    }
}
