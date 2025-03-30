package com.jp.markethub.consumer;

import com.jp.markethub.MarketHub;
import com.jp.markethub.consumer.feature.BidOfferLastPrice;
import com.jp.markethub.consumer.feature.FeatureContract;
import com.jp.markethub.transport.TcpPublisher;

import java.util.HashMap;
import java.util.Map;

public class ConsumerFactory {
    private static final Map<Integer, FeatureContract> consumers = new HashMap<>();


    public static void initialize(MarketHub hub){
        consumers.put(10000, new BidOfferLastPrice(hub, new TcpPublisher(hub, 10000)));
    }

    public static FeatureContract getConsumer(int port) {
        FeatureContract consumer = consumers.get(port);
        if (consumer != null) {
            return consumer;
        }
        throw new UnsupportedOperationException("No consumer registered for port " + port);
    }

    public static Map<Integer, FeatureContract> getExposedPorts() {
        return consumers;
    }
}
