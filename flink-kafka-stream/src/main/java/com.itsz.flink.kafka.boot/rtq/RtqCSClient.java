package com.itsz.flink.kafka.boot.rtq;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;


@Component
public class RtqCSClient {

    private RtqConnectorManager manager;

    @Autowired
    private KafkaTemplate kafkaTemplate;


    public void startup() {
        manager = new RtqConnectorManager();
        manager.addConnector(kafkaTemplate, RtqTopic.STOCK_SNOTSHOT_RTQ_SUB_REQ_MSG);
        manager.addConnector(kafkaTemplate, RtqTopic.BROKER_QUENE_RTQ_SUB_REQ_MSG);
        manager.addConnector(kafkaTemplate, RtqTopic.HK_INDEX_RTQ_SUB_REQ_MSG);
        manager.startup();
    }

    public boolean isConnected() {
        return manager.isConnected();
    }


}
