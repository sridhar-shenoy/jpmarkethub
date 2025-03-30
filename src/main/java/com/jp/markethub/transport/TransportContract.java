package com.jp.markethub.transport;

public interface TransportContract {
    void publish(byte[] data, int length);

    Integer getPort();
}
