package com.itsz.flink.fraud.detection;

import com.typesafe.sslconfig.util.LoggerFactory;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.logging.Logger;

public class AlertSink implements SinkFunction<Alert> {
    public static final Logger logger = Logger.getLogger(AlertSink.class.getName());

    @Override
    public void invoke(Alert alert, Context context) throws Exception {
        logger.info("fraud detection found: accountId =" + alert.getId());
    }
}
