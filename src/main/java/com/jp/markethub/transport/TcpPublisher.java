package com.jp.markethub.transport;

import com.jp.markethub.MarketHub;
import com.jp.markethub.consumer.ConsumerManager;
import com.jp.markethub.log.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class TcpPublisher implements TransportContract {

    private final Logger logger = Logger.getInstance();
    protected final MarketHub hub;
    private final int port;
    protected final List<SocketChannel> clients = new CopyOnWriteArrayList<>();
    private volatile boolean running = true;


    public TcpPublisher(MarketHub hub, int port) {
        this.hub = hub;
        this.port = port;
    }

    @Override
    public void publish(byte[] data, int length) {
        if(logger.isDebugEnabled()) {
            logger.debug(getClass(), "Received data [ " + new String(data).trim() + " ] to publish");
        }
        ByteBuffer buffer = ByteBuffer.wrap(data, 0, length);
        ConsumerManager consumerManager = hub.getConsumerManagerForPort(port);
        if(consumerManager == null)return;
        Iterator<SocketChannel> iterator = consumerManager.getClients();
        while (iterator.hasNext()) {
            SocketChannel client = iterator.next();
            try {
                buffer.rewind(); // Reset buffer position for each client
                while (buffer.hasRemaining()) {
                    client.write(buffer);
                    if(logger.isDebugEnabled()) {
                        logger.debug(getClass(), "Published data [ " + new String(data).trim() + "] to [ " + client.getRemoteAddress().toString().trim() + " ]");
                    }
                }
            } catch (IOException e) {
                logger.error(getClass(), "Failed to write to client: " + e.getMessage());
                iterator.remove();
                closeClient(client);
            }
        }
    }

    public int getTotalClients() {
        return clients.size();
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

    public void addClient(SocketChannel client) {
        clients.add(client);
        logger.debug(getClass(), "New client connected [ " + client + " ]. Total clients: " + clients.size());
    }

    public void removeClient(SocketChannel channel) {
        clients.remove(channel);
        logger.debug(getClass(), "Client removed. Total clients: " + getTotalClients());
    }
}
