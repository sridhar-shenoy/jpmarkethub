package com.jp.markethub;

public enum ProducerType {
    BIDOFFER(0),
    LASTPRICE(1);


    private final int sequenceId;

    ProducerType(int sequenceId) {
        this.sequenceId = sequenceId;
    }

    public int getSequenceId() {
        return sequenceId;
    }
}