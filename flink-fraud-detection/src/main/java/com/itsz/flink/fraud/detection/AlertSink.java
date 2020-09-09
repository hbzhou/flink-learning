package com.itsz.flink.fraud.detection;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AlertSink implements SinkFunction<Alert> {
    public static final Logger logger = LoggerFactory.getLogger(AlertSink.class.getName());

    @Override
    public void invoke(Alert alert, Context context) throws Exception {
        logger.info("fraud detection found: accountId =" + alert.getId());
    }
}
