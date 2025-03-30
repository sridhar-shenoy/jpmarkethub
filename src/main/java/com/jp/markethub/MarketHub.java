package com.jp.markethub;

import com.jp.markethub.consumer.AbstractConsumer;
import com.jp.markethub.consumer.ConsumerFactory;
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
    private static MarketHub instance;
    private static final Logger logger = Logger.getInstance();
    private ExecutorService executor;
    private Selector selector;
    private final Map<ProducerType, Producer> producers = new ConcurrentHashMap<>();
    private final Map<SocketChannel, AbstractConsumer> consumers = new ConcurrentHashMap<>();
    private final Map<Integer, ServerSocketChannel> consumerServers = new ConcurrentHashMap<>();
    private volatile boolean running = true;


    public void startConsumer() throws IOException {
        logger.info(MarketHub.class, "Starting MarketHub server...");
        if (executor == null || executor.isShutdown()) {
            executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        }
        ConsumerFactory.initialize(this);
        selector = Selector.open();

        // Start consumer servers for each exposed port
        for (Map.Entry<Integer, AbstractConsumer> entry : ConsumerFactory.getExposedPorts().entrySet()) {
            startConsumerServer(entry.getKey());
        }

        executor.execute(this::runSelectorLoop);
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
                logger.debug(MarketHub.class, "Selector closed normally");
                break;
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

    private void handleAccept(SelectionKey key) throws IOException {
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        try {
            SocketChannel client = serverChannel.accept();
            client.configureBlocking(false);

            int port = ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
            AbstractConsumer consumer = ConsumerFactory.getConsumer(port);
            consumer.addClient(client);
            consumers.put(client, consumer);

            if (consumer.getTotalClients() == 1) {
                new Thread(consumer, "Consumer").start();
            }
            logger.debug(MarketHub.class, "New consumer connected: " + client.getRemoteAddress());
        } catch (Exception e) {
            logger.error(MarketHub.class, e.getMessage());
        }
    }


    private void closeClientChannel(SocketChannel channel) {
        try {
            AbstractConsumer consumer = consumers.remove(channel);
            if (consumer != null) {
                consumer.removeClient(channel);
            }
            channel.close();
        } catch (IOException e) {
            logger.error(MarketHub.class, "Error closing channel: " + e.getMessage());
        }
    }

    public void connectToProducer(ProducerType type, int port) throws IOException {
        Producer producer = new Producer(type, port);
        producers.put(type, producer);
        producer.connect();
        logger.info(MarketHub.class, "Connected to " + type + " producer on port " + port);
    }

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

    private void closeAllResources() {
        // Close all consumer channels
        consumers.keySet().forEach(this::closeClientChannel);

        consumers.values().forEach(AbstractConsumer::stop);

        // Close all producer connections
        producers.values().forEach(Producer::disconnect);

        // Close server channels
        consumerServers.values().forEach(server -> {
            try {
                server.close();
            } catch (IOException e) {
                logger.error(MarketHub.class, "Error closing server channel: " + e.getMessage());
            }
        });

        logger.info(MarketHub.class, "MarketHub stopped");
    }

    public int getConsumerCount() {
        return consumers.size();
    }
    public Producer getProducer(ProducerType type) { return producers.get(type); }

    public void reset() {
        closeAllResources();
        producers.values().forEach(Producer::reset);
        producers.clear();
        consumers.clear();
        consumerServers.clear();
    }
}