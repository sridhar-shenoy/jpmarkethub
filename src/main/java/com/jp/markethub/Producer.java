package com.jp.markethub;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Producer {
    private final Logger logger = Logger.getInstance();
    private final int port;
    private final ProducerType type;
    private SocketChannel channel;
    private final byte[][] ringBuffer;
    private final int[] lengths; // Tracks valid data length for each slot
    private final Sequencer sequencer = new Sequencer();
    private final int bufferSize = 4096;

    public Producer(ProducerType type, int port) {
        this.type = type;
        this.port = port;
        validateBufferSize(bufferSize);
        this.ringBuffer = new byte[bufferSize][256];
        this.lengths = new int[bufferSize];
    }

    private void validateBufferSize(int size) {
        if ((size & (size - 1)) != 0) {
            throw new IllegalArgumentException("Buffer size must be power-of-two");
        }
    }

    public byte[] getData(long sequence) {
        return ringBuffer[(int) (sequence & (bufferSize - 1))];
    }

    public int getDataLength(long sequence) {
        return lengths[(int) (sequence & (bufferSize - 1))];
    }


    public void connect() throws IOException {
        channel = SocketChannel.open(new InetSocketAddress("localhost", port));
        channel.configureBlocking(false);
        new Thread(this::readData).start();
    }

    public void disconnect() {
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (IOException e) {
            Logger.getInstance().error(Producer.class, "Disconnect error: " + e.getMessage());
        }
    }

    private void readData() {
        logger.debug(Producer.class, "Starting data read loop for " + type + " producer");
        int mask = bufferSize - 1;
        ByteBuffer buffer = ByteBuffer.allocate(256);
        while (true) {
            try {
                int nextSlot = (int) (sequencer.get() & mask);
                byte[] dataSlot = ringBuffer[nextSlot];

                int bytesRead = channel.read(buffer);
                if (bytesRead > 0) {
                    logger.debug(Producer.class, "Received " + bytesRead + " bytes from " + type);
                    buffer.flip();
                    buffer.get(dataSlot, 0, bytesRead); // Copy only the bytes read
                    lengths[nextSlot] = bytesRead;      // Store valid length
                    buffer.clear();                    // Reset buffer for next read
                    sequencer.increment();
                    logger.debug(Producer.class, type + " sequence updated to: " + sequencer.get());
                } else if (bytesRead == -1) {
                    // Handle end of stream (e.g., producer disconnected)
                    break;
                } else {
                    // No data available, yield to prevent busy-waiting
                    Thread.yield();
                }
            } catch (IOException e) {
                logger.error(Producer.class, "Connection error with " + type + " producer: " + e.getMessage());
                break;
            }
        }
    }

    public Sequencer getSequencer() {
        return sequencer;
    }

    public int getBufferSize() {
        return bufferSize;
    }
}