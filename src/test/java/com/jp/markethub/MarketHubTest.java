package com.jp.markethub;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class MarketHubTest {
    public static final int BID_OFFER_LASTPRICE_INTERNAL_PORT = 10000;
    private MarketHub hub;
    private DummyProducer bidOfferProducer;
    private DummyProducer lastPriceProducer;
    private List<DummyConsumer> consumers;

    @Before
    public void setUp() throws Exception {

        new Thread(this::startMarketHub,"setup").start();
        // Initialize consumers list
        consumers = new ArrayList<>();
        // Wait for connections
        Thread.sleep(500);
    }

    private void startMarketHub() {
        // Initialize MarketHub
        hub = new MarketHub();
        try {
            hub.startConsumer();

            // Start dummy producers
            bidOfferProducer = new DummyProducer(9000);
            lastPriceProducer = new DummyProducer(9001);
            bidOfferProducer.start();
            lastPriceProducer.start();

            // Connect MarketHub to producers
            hub.connectToProducer(ProducerType.BIDOFFER, 9000);
            hub.connectToProducer(ProducerType.LASTPRICE, 9001);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        // Close all consumers
        consumers.forEach(DummyConsumer::close);

        // Shutdown MarketHub
        hub.stop();

        // Stop dummy producers
        bidOfferProducer.stop();
        lastPriceProducer.stop();

        // Add cleanup delay
        Thread.sleep(200);
    }


    @Test
    public void testBidAndOfferAndLastPriceUpdateToSingleConsumer() throws Exception {
        try (DummyConsumer jpStride = new DummyConsumer(BID_OFFER_LASTPRICE_INTERNAL_PORT, "JP Stride")) {
            // Initialization delay for consumer connection
            Thread.sleep(100);

            // Send initial data
            bidOfferProducer.publish("4,103.0,104.0\n");
            lastPriceProducer.publish("4,103.5\n");

            // Verify initial bid/offer
            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);
            assertEquals("4,103.0,104.0,", jpStride.getNextMessage(2, TimeUnit.SECONDS));

            // Verify combined update
            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);
            assertEquals("4,103.0,104.0,103.5", jpStride.getNextMessage(2, TimeUnit.SECONDS));

            // Disconnect bid producer
            bidOfferProducer.stop();

            // Send new last price
            lastPriceProducer.publish("5,104.0\n");

            // Verify final update
            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);
            String result = jpStride.getNextMessage(2, TimeUnit.SECONDS);
            assertEquals("4,103.0,104.0,104.0", result);

            // Ensure no residual messages
            assertEquals(0, jpStride.allMessageCount());
        }
    }

    @Test
    public void testBidAndOfferUpdateOnlyToSingleConsumer() throws Exception {
        try (DummyConsumer jpStride = new DummyConsumer(BID_OFFER_LASTPRICE_INTERNAL_PORT, "JP Stride")) {
            // 1. Let consumer connect and initialize
            Thread.sleep(100); // Short initialization delay

            // 2. Publish test data
            bidOfferProducer.publish("4,103.0,104.0\n");

            // 3. Wait for message delivery with timeout
            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);

            // 4. Verify received content
            String message = jpStride.getNextMessage(100, TimeUnit.MILLISECONDS);
            assertEquals("4,103.0,104.0,", message);

            // 5. Verify no extra messages
            assertEquals(0, jpStride.allMessageCount());
        }
    }

    @Test
    public void testBidAndOfferAndLastPriceUpdateToMultipleConsumer() throws Exception {
        try (
                DummyConsumer consumer1 = new DummyConsumer(BID_OFFER_LASTPRICE_INTERNAL_PORT, "JP Stride");
                DummyConsumer consumer2 = new DummyConsumer(BID_OFFER_LASTPRICE_INTERNAL_PORT, "JP Algo");
        ) {
            consumers.addAll(Arrays.asList(consumer1,consumer2));

            // Wait for all consumers to connect
            while (hub.getConsumerCount() < 2) {
                Thread.sleep(100);
            }

            bidOfferProducer.publish("4,103.0,104.0\n");
            lastPriceProducer.publish("4,103.5\n");

            for (DummyConsumer consumer : consumers) {
                consumer.awaitFirstMessage(2, TimeUnit.SECONDS);
                assertEquals("4,103.0,104.0,", consumer.getNextMessage(2, TimeUnit.SECONDS));
                consumer.awaitFirstMessage(2, TimeUnit.SECONDS);
                assertEquals("4,103.0,104.0,103.5", consumer.getNextMessage(2, TimeUnit.SECONDS));
                assertEquals(0, consumer.allMessageCount());
            }
        }
    }


    private void waitForConnections(int expected) throws InterruptedException {
        int retries = 0;
        while (hub.getConsumerCount() < expected && retries++ < 10) {
            Thread.sleep(100);
        }
        assertEquals("All consumers should connect", expected, hub.getConsumerCount());
    }




}