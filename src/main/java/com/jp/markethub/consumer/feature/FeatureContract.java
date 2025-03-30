package com.jp.markethub.consumer.feature;

import com.jp.markethub.producer.ProducerType;
import com.jp.markethub.transport.TcpPublisher;
import com.jp.markethub.transport.TransportContract;

import java.util.EnumSet;

public interface FeatureContract {
    void onUpdate(byte[] data, int length, ProducerType type);

    EnumSet<ProducerType> getInterestList();

    TransportContract getTcpPublisher();
}
