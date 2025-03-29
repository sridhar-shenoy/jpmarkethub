package com.jp.markethub;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    public void addClient(SocketChannel client) {
        clients.add(client);
        logger.debug(getClass(), "New client connected [ " + client + " ]. Total clients: " + clients.size());
    }

    public void removeClient(SocketChannel channel) {
        clients.remove(channel);
        logger.debug(getClass(), "Client removed. Total clients: " + getTotalClients());
    }

    public void run() {
        logger.debug(getClass(), "Starting consumer thread");
        Map<ProducerType, Long> lastSequences = new ConcurrentHashMap<>();

        while (running) {
            for (ProducerType type : interests) {
                Producer producer = hub.getProducer(type);
                if (producer == null) continue;

                Sequencer sequencer = producer.getSequencer();
                long currentSeq = sequencer.get();
                long lastSeq = lastSequences.getOrDefault(type, -1L);

                if (currentSeq <= lastSeq) continue;

                int bufferSize = producer.getBufferSize();
                long availableCount = currentSeq - lastSeq;

                // Handle buffer wrap-around scenario
                if (availableCount > bufferSize) {
                    long newStart = Math.max(0, currentSeq - bufferSize + 1);
                    logger.info(getClass(), "Buffer overflow detected. Jumping to sequence " + newStart);
                    lastSeq = newStart - 1;
                }

                // Process all pending sequences
                for (long seq = lastSeq ; seq <= currentSeq; seq++) {
                    int index = (int) (seq & (bufferSize - 1)); // Fast modulo for power-of-two sizes
                    byte[] data = producer.getData(index);
                    int length = producer.getDataLength(seq);

                    if (length > 0) {
                        onUpdate(data, length, type);
                    }
                }

                lastSequences.put(type, currentSeq);
            }
            sleepSafely();
        }
    }

    public int getTotalClients() {
        return clients.size();
    }

    abstract void onUpdate(byte[] data, int length, ProducerType type);

    protected void publish(byte[] data, int length) {
        logger.info(getClass(), "Published data " + new String(data));
        ByteBuffer buffer = ByteBuffer.wrap(data, 0, length);
        Iterator<SocketChannel> iterator = clients.iterator();

        while (iterator.hasNext()) {
            SocketChannel client = iterator.next();
            try {
                buffer.rewind(); // Reset buffer position for each client
                while (buffer.hasRemaining()) {
                    client.write(buffer);
                    logger.info(getClass(), "Published to " + client.getRemoteAddress());
                }
            } catch (IOException e) {
                logger.error(getClass(), "Failed to write to client: " + e.getMessage());
                iterator.remove();
                closeClient(client);
            }
        }
    }

    private void closeClient(SocketChannel client) {
        try {
            client.close();
        } catch (IOException ex) {
            logger.error(getClass(), "Error closing client: " + ex.getMessage());
        }
    }

    private void sleepSafely() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            running = false;
            Thread.currentThread().interrupt();
        }
    }

    public void stop() {
        running = false;
        clients.forEach(this::closeClient);
    }
}