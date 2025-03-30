package com.jp.markethub.producer;

import com.jp.markethub.log.Logger;
import com.jp.markethub.common.Sequencer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

public class Producer {
    private final Logger logger = Logger.getInstance();

    private SocketChannel channel;

    private final int port;
    private final int[] lengths; // Tracks valid data length for each slot
    private final byte[][] ringBuffer;
    private final int bufferSize = (int) Math.pow(2, 14);
    private final ProducerType type;
    private final Sequencer sequencer = new Sequencer();

    private byte[] accumulatedData = new byte[0]; // Accumulates data across reads
    private long firstDataTime = -1;

    public Producer(ProducerType type, int port) {
        this.type = type;
        this.port = port;
        this.ringBuffer = new byte[bufferSize][256];
        this.lengths = new int[bufferSize];
        validateBufferSize(bufferSize);
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
            logger.error(Producer.class, "Disconnect error: " + e.getMessage());
        }
    }

    private void readData() {
        if (logger.isDebugEnabled()) {
            logger.debug(Producer.class, "Starting data read loop for " + type + " producer");
        }
        int mask = bufferSize - 1;
        ByteBuffer tempBuffer = ByteBuffer.allocate((int) Math.pow(2,10));
        try {
            while (channel.isOpen() && !Thread.interrupted()) {
                tempBuffer.clear();
                int bytesRead = channel.read(tempBuffer);
                if (bytesRead > 0) {
                    if (firstDataTime == -1) {
                        firstDataTime = System.currentTimeMillis();
                    }
                    tempBuffer.flip();
                    byte[] newData = new byte[tempBuffer.remaining()];
                    tempBuffer.get(newData);

                    // Append new data to accumulatedData
                    byte[] combined = new byte[accumulatedData.length + newData.length];
                    System.arraycopy(accumulatedData, 0, combined, 0, accumulatedData.length);
                    System.arraycopy(newData, 0, combined, accumulatedData.length, newData.length);
                    accumulatedData = combined;

                    int start = 0;
                    for (int i = 0; i < accumulatedData.length; i++) {
                        if (accumulatedData[i] == ';') {
                            int messageLength = i - start;
                            if (messageLength > 0) {
                                int nextSlot = (int) (sequencer.get() & mask);
                                byte[] slot = ringBuffer[nextSlot];
                                int copyLength = Math.min(messageLength, slot.length);
                                System.arraycopy(accumulatedData, start, slot, 0, copyLength);
                                lengths[nextSlot] = copyLength;
                                sequencer.increment();
                                if (logger.isDebugEnabled()) {
                                    logger.debug(Producer.class, type + " sequence updated to: " + sequencer.get());
                                }
                            }
                            start = i + 1;
                        }
                    }

                    if (start < accumulatedData.length) {
                        accumulatedData = Arrays.copyOfRange(accumulatedData, start, accumulatedData.length);
                    } else {
                        accumulatedData = new byte[0];
                    }
                } else if (bytesRead == -1) {
                    break;
                }
            }
        } catch (IOException e) {
            logger.error(Producer.class, "Connection error with " + type + " producer: " + e.getMessage());
        } finally {
            disconnect();
        }
    }

    public Sequencer getSequencer() {
        return sequencer;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void reset() {
        Arrays.fill(lengths, 0);
        sequencer.set(0);
        accumulatedData = new byte[0];
    }

    public long getFirstDataTime() {
        return firstDataTime;
    }
}