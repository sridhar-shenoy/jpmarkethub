package com.jp.markethub;

public class Sequencer {
    private volatile long sequence = 0;

    public long get() {
        return sequence;
    }

    public synchronized void increment() {
        sequence++;
    }

    public synchronized void set(long value) {
        sequence = value;
    }
}