package com.jp.markethub;

import com.jp.markethub.config.MarketHubConfig;
import com.jp.markethub.consumer.ConsumerManager;
import com.jp.markethub.consumer.ConsumerFactory;
import com.jp.markethub.consumer.feature.FeatureContract;
import com.jp.markethub.log.Logger;
import com.jp.markethub.producer.Producer;
import com.jp.markethub.producer.ProducerType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MarketHub {
    private static final Logger logger = Logger.getInstance();
    private final MarketHubConfig config;


    private ExecutorService executor;
    private Selector selector;

    private final Map<ProducerType, Producer> producers = new ConcurrentHashMap<>();
    private final Map<SocketChannel, FeatureContract> consumers = new ConcurrentHashMap<>();
    private final Map<Integer, ServerSocketChannel> consumerServers = new ConcurrentHashMap<>();
    private final Map<Integer, ConsumerManager> consumerManagerMap = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    public MarketHub(MarketHubConfig config) {
        this.config = config;
    }

    public void startConsumer() throws IOException {
        logger.info(MarketHub.class, "Starting MarketHub server...");
        ConsumerFactory.initialize(this);

        if (executor == null || executor.isShutdown()) {
            executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        }

        selector = Selector.open();

        //-- Start consumer servers for each publicly exposed port
        for (Map.Entry<Integer, FeatureContract> entry : ConsumerFactory.getExposedPorts().entrySet()) {
            startConsumerServer(entry.getKey());
        }

        submit(this::runSelectorLoop);
    }

    public void submit(Runnable runSelectorLoop) {
        executor.execute(runSelectorLoop);
    }

    private void startConsumerServer(int port) throws IOException {
        ServerSocketChannel serverChannel = ServerSocketChannel.open();
        serverChannel.bind(new InetSocketAddress(port));
        serverChannel.configureBlocking(false);

        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        consumerServers.put(port, serverChannel);
        logger.info(MarketHub.class, "Listening on consumer port " + port);
    }

    private void runSelectorLoop() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                selector.select();
                processSelectedKeys();
            } catch (ClosedSelectorException e) {
                logger.error(MarketHub.class, "Selector closed normally");
            } catch (IOException e) {
                logger.error(MarketHub.class, "Selector error: " + e.getMessage());
            }
        }
    }

    private void processSelectedKeys() throws IOException {
        Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
        while (keys.hasNext()) {
            SelectionKey key = keys.next();
            keys.remove();

            if (!key.isValid()) continue;

            if (key.isAcceptable()) {
                handleAccept(key);
            }
        }
    }

    private void handleAccept(SelectionKey key) {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        try {
            SocketChannel client = serverChannel.accept();
            client.configureBlocking(false);

            //-- Identify the port, and then we can wire the connection to its associated feature
            int port = ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();

            //-- Every port is associated with a feature
            FeatureContract feature = ConsumerFactory.getConsumer(port);

            //-- Wire the three
            wireClientFeatureToPort(client, feature, port);

            if (logger.isDebugEnabled()) {
                logger.debug(MarketHub.class, "New consumer connected: " + client.getRemoteAddress());
            }
        } catch (Exception e) {
            logger.error(MarketHub.class, e.getMessage());
        }
    }

    private void wireClientFeatureToPort(SocketChannel client, FeatureContract feature, int port) {
        consumers.put(client, feature);
        consumerManagerMap.putIfAbsent(port, new ConsumerManager(this));
        ConsumerManager consumerManager = consumerManagerMap.get(port);
        consumerManager.addClient(client);
        consumerManager.registerInterest(feature.getInterestList());
        consumerManager.registerFeature(feature);
        consumerManager.start();
    }

    private void closeClientChannel(SocketChannel channel) {
        try {
            FeatureContract consumer = consumers.remove(channel);
            if (consumer != null) {
                int port = ((InetSocketAddress) channel.getLocalAddress()).getPort();
                ConsumerManager consumerManager = consumerManagerMap.get(port);
                consumerManager.removeClient(channel);
            }
            channel.close();
        } catch (IOException e) {
            logger.error(MarketHub.class, "Error closing channel: " + e.getMessage());
        }
    }

    //-- This must be invoked externally via Autosys or other job
    public void connectToProducer(ProducerType type, int port) throws IOException {
        Producer producer = new Producer(type, port,config);
        producers.put(type, producer);
        producer.connect();
        if (logger.isDebugEnabled()) {
            logger.debug(MarketHub.class, "Connected to " + type + " producer on port " + port);
        }
    }

    // -- Stop the hub
    public void stop() {
        logger.info(MarketHub.class, "Stopping MarketHub App");
        running = false;
        if (executor != null) {
            executor.shutdown(); // Graceful shutdown
            try {
                if (!executor.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        try {
            if (selector != null) {
                selector.close();
            }
            closeAllResources();
        } catch (IOException e) {
            logger.error(MarketHub.class, "Shutdown error: " + e.getMessage());
        }
    }

    private void closeAllResources() throws IOException {
        consumers.keySet().forEach(this::closeClientChannel);
        consumerManagerMap.keySet().forEach(port -> {
            ConsumerManager consumerManager = consumerManagerMap.get(port);
            consumerManager.close();
        });
        producers.values().forEach(Producer::disconnect);
        for (ServerSocketChannel server : consumerServers.values()) {
            server.close();
        }
        ConsumerFactory.reset();
        logger.info(MarketHub.class, "MarketHub stopped");
    }

    public Producer getProducer(ProducerType type) {
        return producers.get(type);
    }

    public void reset() throws IOException {
        closeAllResources();
        producers.values().forEach(Producer::reset);
        producers.clear();
        consumers.clear();
        consumerServers.clear();
    }

    public ConsumerManager getConsumerManagerForPort(int port) {
        return consumerManagerMap.get(port);
    }

    public long getFirstDataTime(ProducerType type) {
        return producers.get(type).getFirstDataTime();
    }
}