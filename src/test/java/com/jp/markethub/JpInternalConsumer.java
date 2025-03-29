package com.jp.markethub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.*;

public class JpInternalConsumer implements AutoCloseable {
    private static final Logger logger = Logger.getInstance();
    private final BlockingQueue<String> receivedMessages = new LinkedBlockingDeque<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private Socket socket;
    private final int port;
    private final String appName;
    private volatile boolean running = true;
    private final CountDownLatch connectionEstablishedLatch = new CountDownLatch(1); // New latch


    private final CountDownLatch messageReceivedLatch = new CountDownLatch(1);

    public JpInternalConsumer(int port, String appName) {
        this.port = port;
        this.appName = appName;
    }

    public void connectToMarketHubAndListen() throws IOException {
        this.socket = new Socket("localhost", port);
        connectionEstablishedLatch.countDown(); // Signal connection success

        executor.execute(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()))) {

                logger.debug(JpInternalConsumer.class, "Starting to listen for messages for [ " + appName + " ]");
                while (running && !socket.isClosed()) {
                    String message = reader.readLine();
                    if (message != null) {
                        receivedMessages.add(message);
                        messageReceivedLatch.countDown();
                        logger.debug(JpInternalConsumer.class, "Received message: " + message + " for [ " + appName + " ]");
                    }
                }
            } catch (IOException e) {
                if (running) {
                    logger.error(JpInternalConsumer.class, "Read error: " + e.getMessage());
                }
            }
        });
    }

    public void awaitFirstMessage(long timeout, TimeUnit unit)
            throws InterruptedException {
        messageReceivedLatch.await(timeout, unit);
    }

    public boolean awaitConnectionToMarketHub(long timeout, TimeUnit unit) throws InterruptedException {
        return connectionEstablishedLatch.await(timeout, unit);
    }

    public String getNextMessage(long timeout, TimeUnit unit) 
        throws InterruptedException {
        return receivedMessages.poll(timeout, unit);
    }


    @Override
    public void close() {
        running = false;
        executor.shutdownNow();
        try {
            if (!socket.isClosed()) {
                socket.close();
            }
        } catch (IOException e) {
            logger.error(JpInternalConsumer.class, "Close error: " + e.getMessage());
        }
    }

    public int allMessageCount() {
        return receivedMessages.size();
    }
}