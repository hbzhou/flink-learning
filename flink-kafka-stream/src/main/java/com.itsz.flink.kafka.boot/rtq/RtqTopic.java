package com.itsz.flink.kafka.boot.rtq;


import java.util.HashMap;
import java.util.Map;

public interface RtqTopic {

    String STOCK_SNOTSHOT_RTQ_SUB_REQ_MSG = "110:ALL@XHKG;";
    String STOCK_STATUS_RTQ_SUB_REQ_MSG = "115:ALL@XHKG;";
    String BROKER_QUENE_RTQ_SUB_REQ_MSG = "130:ALL@XHKG;";
    String HK_INDEX_RTQ_SUB_REQ_MSG = "210:.HSCC,.HSI,.HSCE;";
    String HK_DELAY_INDEX_RTQ_SUB_REQ_MSG = "211:ALL@XHKG;";
    String CONNECT_MARKET_TURNOVER_RTQ_SUB_REQ_MSG = "435:ALL;";
    String WORLD_INDEX_RTQ_SUB_REQ_MSG = "710:ALL;";

    //kafka
    String RTQ_STREAM_TOPIC = "cache.server.rtq";
    String BORKER_QUEUE_TOPIC = "cache.server.broker.queue";
    String INDEX_TOPIC = "cache.server.index";

    Map<String, String> topicMap = new HashMap() {
        {
            put(STOCK_SNOTSHOT_RTQ_SUB_REQ_MSG, RTQ_STREAM_TOPIC);
            put(BROKER_QUENE_RTQ_SUB_REQ_MSG, BORKER_QUEUE_TOPIC);
            put(HK_INDEX_RTQ_SUB_REQ_MSG, INDEX_TOPIC);

        }
    };


}
