package com.jp.markethub.consumer.feature;

import com.jp.markethub.MarketHub;
import com.jp.markethub.log.Logger;
import com.jp.markethub.producer.ProducerType;

import java.nio.channels.SocketChannel;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class AbstractConsumer implements Runnable {
    private final Logger logger = Logger.getInstance();
    protected final MarketHub hub;
    private final EnumSet<ProducerType> interests;
    protected final List<SocketChannel> clients = new CopyOnWriteArrayList<>();
    private volatile boolean running = true;

    public AbstractConsumer(MarketHub hub, EnumSet<ProducerType> interests) {
        this.hub = hub;
        this.interests = interests;
    }
}