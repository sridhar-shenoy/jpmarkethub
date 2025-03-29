package com.jp.markethub;

import java.util.HashMap;
import java.util.Map;

public class ConsumerFactory {
    private static final Map<Integer, AbstractConsumer> consumers = new HashMap<>();


    public static void initialize(MarketHub hub){
        consumers.put(10000, new BidOfferLastPrice(hub));
    }

    public static AbstractConsumer getConsumer(int port) {
        AbstractConsumer consumer = consumers.get(port);
        if (consumer != null) {
            return consumer;
        }
        throw new UnsupportedOperationException("No consumer registered for port " + port);
    }

    public static Map<Integer, AbstractConsumer> getExposedPorts() {
        return consumers;
    }
}
