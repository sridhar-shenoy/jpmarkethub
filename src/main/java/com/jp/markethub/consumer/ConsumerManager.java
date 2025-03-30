package com.jp.markethub.consumer;

import com.jp.markethub.MarketHub;
import com.jp.markethub.common.Sequencer;
import com.jp.markethub.consumer.feature.FeatureContract;
import com.jp.markethub.log.Logger;
import com.jp.markethub.producer.Producer;
import com.jp.markethub.producer.ProducerType;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerManager implements Runnable {
    private final Logger logger = Logger.getInstance();
    private final MarketHub marketHub;
    private EnumSet<ProducerType> interests;
    protected final List<SocketChannel> clients = new CopyOnWriteArrayList<>();
    private volatile boolean running = true;
    private FeatureContract feature;
    private AtomicBoolean isThreadStarted = new AtomicBoolean(false);

    public ConsumerManager(MarketHub marketHub) {
        this.marketHub = marketHub;
    }

    public void registerInterest(EnumSet<ProducerType> interests) {
        this.interests = interests;
    }

    @Override
    public void run() {
        logger.debug(getClass(), "Starting consumer thread " + this);
        long[] lastSequence = new long[ProducerType.values().length];

        while (running) {
            for (ProducerType type : interests) {
                Producer producer = marketHub.getProducer(type);
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
                    logger.info(getClass(), "Consumer Manager sequence =[ " + lastSeq + "] for Producer = [ " + producer + "] index = [ " + index + " ] length =[ " + length + " ]  data =[ " + new String(data).trim() + " ] ");
                    feature.onUpdate(data, length, type);
                }

                //-- store the last sequence
                lastSequence[type.getSequenceId()] = lastSeq+1;
            }
        }
    }


    public void addClient(SocketChannel client) {
        clients.add(client);
        logger.debug(getClass(), "New client connected [ " + client + " ]. Total clients: " + clients.size());

    }

    public void removeClient(SocketChannel channel) {
        clients.remove(channel);
        logger.debug(getClass(), "Client removed. Total clients: " + getTotalClients());
    }

    public int getTotalClients() {
        return clients.size();
    }

    public void registerFeature(FeatureContract feature) {
        this.feature = feature;
    }

    public void start() {
        if (isThreadStarted.compareAndSet(false,true)) {
            logger.info(getClass(),"Clients = " + clients);
            new Thread(this, feature.getClass().getSimpleName()).start();
        }
    }

    public MarketHub getMarketHub(){
        return marketHub;
    }

    public void close() throws IOException {
        for (SocketChannel client : clients) client.close();
    }

    public Iterator<SocketChannel> getClients() {
        return clients.iterator();
    }
}
