package com.jp.markethub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.*;

public class DummyConsumer implements AutoCloseable {
    private static final Logger logger = Logger.getInstance();
    private final BlockingQueue<String> receivedMessages = new LinkedBlockingDeque<>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Socket socket;
    private final String appName;
    private volatile boolean running = true;

    private final CountDownLatch messageReceivedLatch = new CountDownLatch(1);

    public DummyConsumer(int port, String appName) throws IOException {
        this.socket = new Socket("localhost", port);
        this.appName = appName;
        startListening();
    }

    private void startListening() {
        executor.execute(() -> {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()))) {

                logger.debug(DummyConsumer.class, "Starting to listen for messages for [ " + appName + " ]");
                while (running && !socket.isClosed()) {
                    String message = reader.readLine();
                    if (message != null) {
                        receivedMessages.add(message);
                        messageReceivedLatch.countDown(); // Signal message received
                        logger.debug(DummyConsumer.class, "Received message: " + message + " for [ " + appName + " ]");
                    }
                }
            } catch (IOException e) {
                if (running) {
                    logger.error(DummyConsumer.class, "Read error: " + e.getMessage());
                }
            }
        });
    }

    public void awaitFirstMessage(long timeout, TimeUnit unit)
            throws InterruptedException {
        messageReceivedLatch.await(timeout, unit);
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
            logger.error(DummyConsumer.class, "Close error: " + e.getMessage());
        }
    }

    public int allMessageCount() {
        return receivedMessages.size();
    }
}