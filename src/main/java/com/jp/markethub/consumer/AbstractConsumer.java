package com.jp.markethub.consumer;

import com.jp.markethub.log.Logger;
import com.jp.markethub.MarketHub;
import com.jp.markethub.common.Sequencer;
import com.jp.markethub.producer.Producer;
import com.jp.markethub.producer.ProducerType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.EnumSet;
import java.util.Iterator;
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
         long[] lastSequence = new long[ProducerType.values().length];

        while (running) {
            for (ProducerType type : interests) {
                Producer producer = hub.getProducer(type);
                if (producer == null) continue;

                //-- Collect the current and available Producer Sequence
                Sequencer sequencer = producer.getSequencer();
                long currentSeq = sequencer.get();
                long lastSeq = lastSequence[type.getSequenceId()];

                //-- If no data is available go to next producer
                if (currentSeq <= lastSeq) continue;

                //-- If  there is data check the lag
                long bufferSize = producer.getBufferSize();
                long availableCount = currentSeq - lastSeq;

                //-- Handle buffer wrap-around scenario
                if (availableCount > bufferSize) {
                    long newStart = Math.max(0, currentSeq - bufferSize + 1);
                    logger.info(getClass(), "Missed multiple data feeds, Jumping sequence bufferSize =[" + bufferSize + "] availableCount=[" + availableCount + "]" + newStart);
                    lastSeq = newStart - 1;
                }

                //-- Pick the data without any lock
                long index = (int) (lastSeq & (bufferSize - 1));
                byte[] data = producer.getData(index);
                int length = producer.getDataLength(index);

                //-- Update consumer handler
                if (length > 0) {
                    logger.info(getClass(),"data=" + new String(data));
                    onUpdate(data, length, type);
                }

                //-- store the last sequence
                lastSequence[type.getSequenceId()] = lastSeq+1;
            }
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


    public void stop() {
        running = false;
        clients.forEach(this::closeClient);
    }
}