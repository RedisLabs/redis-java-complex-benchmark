package com.redislabs.benchmark.common;

/**
 * Created by Guy on 8/18/2014.
 */

/**
 * Class that simulate transaction
 */
public class BusinessTransaction {

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    private Long timestamp;
    private Double amount;

}
