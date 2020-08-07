package com.itsz.flink.fraud.detection;


import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class TransactionSource implements SourceFunction<Transaction> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Transaction> sourceContext) throws Exception {
        while (isRunning) {
            Transaction transaction = new Transaction();
            int random = new Random().nextInt(20);
            transaction.setAccountId("key" + random);
            transaction.setAmount(0.01);
            transaction.setTimestamp(System.currentTimeMillis());
            sourceContext.collect(transaction);
            if (random == 1 || random == 3) {
                Transaction transaction1 = new Transaction();
                transaction1.setAccountId("key" + random);
                transaction1.setAmount(500 + random);
                transaction1.setTimestamp(System.currentTimeMillis() - 30 * 1000);
                sourceContext.collect(transaction1);
            }

        }
        Thread.sleep(2000);

    }

    @Override
    public void cancel() {
        try {
            Thread.sleep(20000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            isRunning = false;
        }
    }
}
