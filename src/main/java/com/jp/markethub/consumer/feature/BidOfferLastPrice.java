package com.jp.markethub.consumer.feature;

import com.jp.markethub.log.Logger;
import com.jp.markethub.producer.ProducerType;
import com.jp.markethub.transport.TcpPublisher;
import com.jp.markethub.transport.TransportContract;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;

public class BidOfferLastPrice implements FeatureContract {
    private static final int MAX_FIELD_SIZE = 32;

    private final Logger logger = Logger.getInstance();

    private final TransportContract transportContract;
    private final ByteBuffer outputBuffer = ByteBuffer.allocateDirect(128);
    private final byte[] digitBuffer = new byte[10];


    private final byte[] lastBid = new byte[MAX_FIELD_SIZE];
    private int bidLength;
    private final byte[] lastOffer = new byte[MAX_FIELD_SIZE];
    private int offerLength;
    private final byte[] lastPrice = new byte[MAX_FIELD_SIZE];
    private int priceLength;

    private int sequence;

    public BidOfferLastPrice(TransportContract transportContract) {
        this.transportContract = transportContract;
    }

    @Override
    public void onUpdate(byte[] data, int length, ProducerType type) {
        try {
            if (type == ProducerType.BIDOFFER) {
                parseBidOffer(data, length);
            } else if (type == ProducerType.LASTPRICE) {
                parseLastPrice(data, length);
            }

            buildOutputMessage();
            flushOutputBuffer();
        } finally {
            Arrays.fill(data, (byte) 0);
        }
    }

    private void parseBidOffer(byte[] data, int length) {
        int firstComma = -1;
        int secondComma = -1;

        for (int i = 0; i < length; i++) {
            if (data[i] == ',') {
                if (firstComma == -1) {
                    firstComma = i;
                } else if (secondComma == -1) {
                    secondComma = i;
                    break;
                }
            }
        }

        if (secondComma == -1) {
            logger.error(getClass(), "Invalid BIDOFFER format");
            return;
        }

        bidLength = copy(data, firstComma + 1, secondComma, lastBid);
        offerLength = copy(data, secondComma + 1, length, lastOffer);
    }

    private void parseLastPrice(byte[] data, int length) {
        int commaPos = -1;
        for (int i = 0; i < length; i++) {
            if (data[i] == ',') {
                commaPos = i;
                break;
            }
        }

        if (commaPos == -1) {
            logger.error(getClass(), "Invalid LASTPRICE format");
            return;
        }

        priceLength = copy(data, commaPos + 1, length, lastPrice);
    }

    private int copy(byte[] src, int start, int end, byte[] dest) {
        int length = end - start;
        if (length > MAX_FIELD_SIZE) length = MAX_FIELD_SIZE;
        System.arraycopy(src, start, dest, 0, length);
        return length;
    }

    private void buildOutputMessage() {
        outputBuffer.clear();

        // Write sequence number
        writeSequence(sequence++);

        outputBuffer.put((byte) ',');
        outputBuffer.put(lastBid, 0, bidLength);
        outputBuffer.put((byte) ',');
        outputBuffer.put(lastOffer, 0, offerLength);
        outputBuffer.put((byte) ',');
        outputBuffer.put(lastPrice, 0, priceLength);
        outputBuffer.put((byte) '\n');
    }

    private void writeSequence(int value) {
        if (value == 0) {
            outputBuffer.put((byte) '0');
            return;
        }

        int index = digitBuffer.length;
        while (value > 0) {
            digitBuffer[--index] = (byte) ('0' + (value % 10));
            value /= 10;
        }
        outputBuffer.put(digitBuffer, index, digitBuffer.length - index);
    }

    private void flushOutputBuffer() {
        outputBuffer.flip();
        byte[] output = new byte[outputBuffer.remaining()];
        outputBuffer.get(output);
        transportContract.publish(output, output.length);
    }

    @Override
    public EnumSet<ProducerType> getInterestList() {
        return EnumSet.of(ProducerType.BIDOFFER, ProducerType.LASTPRICE);
    }

    @Override
    public TransportContract getTcpPublisher() {
        return transportContract;
    }
}
