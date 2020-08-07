package com.itsz.flink.fraud.detection;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Transaction> transactions = executionEnvironment.addSource(new TransactionSource()).name("transactions");
        SingleOutputStreamOperator<Alert> outputStreamOperator = transactions.keyBy(Transaction::getAccountId).process(new FraudDetector()).name("fraud-detector");
        outputStreamOperator.addSink(new AlertSink());

        executionEnvironment.execute("fraud detection");

    }
}
