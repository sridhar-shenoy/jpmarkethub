package com.jp.markethub.consumer.feature;

import com.jp.markethub.producer.ProducerType;

import java.util.EnumSet;

public interface FeatureContract {
    void onUpdate(byte[] data, int length, ProducerType type);

    EnumSet<ProducerType> getInterestList();
}
