package com.jp.markethub.consumer;

import com.jp.markethub.MarketHub;
import com.jp.markethub.consumer.feature.BidOfferLastPrice;
import com.jp.markethub.consumer.feature.FeatureContract;
import com.jp.markethub.transport.TcpPublisher;
import com.jp.markethub.transport.TransportContract;

import java.util.HashMap;
import java.util.*;

public class ConsumerFactory {
    private static ConsumerFactory instance;
    private final Map<Integer, FeatureContract> featureContracts = new HashMap<>();
    private final List<FeatureContract> featureRegistry = new ArrayList<>();


    private ConsumerFactory() {
    }

    public static synchronized void initialize(MarketHub hub) {
        if (instance != null) {
            throw new IllegalStateException("ConsumerFactory already initialized");
        }
        instance = new ConsumerFactory();
        instance.buildFeatureRegistry(hub);
        instance.buildConsumerMap();
    }

    public static void reset() {
        instance = null;
    }

    private void buildConsumerMap() {
        for (FeatureContract feature : featureRegistry) {
            TransportContract publisher = feature.getTcpPublisher();
            featureContracts.put(publisher.getPort(), feature);
        }
    }

    private void buildFeatureRegistry(MarketHub hub) {
        featureRegistry.add(new BidOfferLastPrice(new TcpPublisher(hub, 10000)));
    }

    public static FeatureContract getConsumer(int port) {
        checkInitialized();
        FeatureContract consumer = instance.featureContracts.get(port);
        if (consumer == null) {
            throw new UnsupportedOperationException("No consumer registered for port " + port);
        }
        return consumer;
    }

    public static Map<Integer, FeatureContract> getExposedPorts() {
        checkInitialized();
        return Collections.unmodifiableMap(instance.featureContracts);
    }

    private static void checkInitialized() {
        if (instance == null) {
            throw new IllegalStateException("ConsumerFactory not initialized. Call initialize() first.");
        }
    }
}
