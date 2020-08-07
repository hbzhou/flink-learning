package com.itsz.flink.fraud.detection;

import lombok.Data;

@Data
public class Transaction {

    private String accountId;

    private long timestamp;

    private double amount;
}
