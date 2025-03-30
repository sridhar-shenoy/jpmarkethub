package com.jp.markethub.mock;

import com.jp.markethub.log.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Bloomberg {
    private final Logger logger = Logger.getInstance();
    private final int port;
    private ServerSocket serverSocket;
    private Socket clientSocket;
    private ExecutorService executor = Executors.newSingleThreadExecutor();
    private volatile boolean running = true;

    public Bloomberg(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        executor.execute(() -> {
            try {
                if(logger.isDebugEnabled()) {
                    logger.debug(Bloomberg.class, "Dummy producer started on port " + port);
                }
                clientSocket = serverSocket.accept();
                if(logger.isDebugEnabled()) {
                    logger.debug(Bloomberg.class, "Accepted MarketHub connection");
                }
            } catch (IOException e) {
                if (running) logger.error(Bloomberg.class, "Accept failed: " + e.getMessage());
            }
        });
    }

    public void publish(String data) {
        if (clientSocket != null && clientSocket.isConnected()) {
            try {
                clientSocket.getOutputStream().write(data.getBytes());
                clientSocket.getOutputStream().flush();
                if(logger.isDebugEnabled()) {
                    logger.debug(Bloomberg.class, "Published data: " + data);
                }
            } catch (IOException e) {
                logger.error(Bloomberg.class, "Write failed: " + e.getMessage());
            }
        }
    }

    public void stop() {
        running = false;
        executor.shutdownNow();
        try {
            if (serverSocket != null) serverSocket.close();
            if (clientSocket != null) clientSocket.close();
        } catch (IOException e) {
            logger.error(Bloomberg.class, "Stop failed: " + e.getMessage());
        }
    }
}