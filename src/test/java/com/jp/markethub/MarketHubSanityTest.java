package com.jp.markethub;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.jp.markethub.TestUtils.waitTillTrue;
import static org.junit.Assert.assertEquals;

public class MarketHubSanityTest extends MarketHubTestBase {

    public static final String JP_STRIDE = "JP Stride";

    @Test
    public void testBidAndOfferUpdateOnlyToSingleConsumer() throws Exception {
        try (JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE)) {

            //-- Connect to Market Hub
            jpStride.connectToMarketHubAndListen();

            //-- Wait till it connects
            jpStride.awaitConnectionToMarketHub(1, TimeUnit.SECONDS);

            //-- Bloomberg now publishes data
            bidOfferFeed.publish("1,103.0,104.0\n");

            //-- Verify that Client has the data
            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);
            String message = jpStride.getNextMessage(1, TimeUnit.SECONDS);
            assertEquals("1,103.0,104.0,", message);

            //-- Ensure there are no messages left to consume
            assertEquals(0, jpStride.allMessageCount());
        }
    }

    @Test
    public void testBidAndOfferAndLastPriceUpdateToSingleConsumer() throws Exception {
        try (JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE)) {

            //-- Connect Internal JP Client to MarketHub for BifOffer and Last Price
            jpStride.connectToMarketHubAndListen();

            //-- Wait till it connects
            jpStride.awaitConnectionToMarketHub(1, TimeUnit.SECONDS);

            //-- Bloomberg now publishes data
            bidOfferFeed.publish("1,103.0,104.0\n");
            lastPriceFeed.publish("1,103.5\n");

            //-- Verify initial bid/offer
            jpStride.awaitFirstMessage(1, TimeUnit.SECONDS);
            assertEquals("1,103.0,104.0,", jpStride.getNextMessage(2, TimeUnit.SECONDS));

            //-- Verify combined update
            waitTillTrue(() -> jpStride.allMessageCount() > 0, 1, TimeUnit.SECONDS);
            assertEquals("1,103.0,104.0,103.5", jpStride.getNextMessage(2, TimeUnit.SECONDS));

            //-- Disconnect BidOffer feed
            bidOfferFeed.stop();

            //-- Send new last price
            lastPriceFeed.publish("2,104.0\n");

            //-- Verify final update with last Known BidOffer and lastPrice
            waitTillTrue(() -> jpStride.allMessageCount() > 0, 500, TimeUnit.SECONDS);
            String result = jpStride.getNextMessage(1, TimeUnit.SECONDS);
            assertEquals("1,103.0,104.0,104.0", result);

            //-- Ensure there are no messages left to consume
            assertEquals(0, jpStride.allMessageCount());
        }
    }

    @Test
    public void testBidAndOfferAndLastPriceUpdateToMultipleConsumer() throws Exception {
        try (
                JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE);
                JpInternalConsumer jpAlgo = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, "JP Algo");
                JpInternalConsumer jpPrimeServices = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, "JP Prime Services");
        ) {
            consumers.addAll(Arrays.asList(jpStride, jpAlgo, jpPrimeServices));
            waitTillAllConsumerAreConnected();

            bidOfferFeed.publish("1,103.0,104.0\n");
            lastPriceFeed.publish("1,103.5\n");

            for (JpInternalConsumer consumer : consumers) {
                consumer.awaitFirstMessage(2, TimeUnit.SECONDS);
                assertEquals("1,103.0,104.0,", consumer.getNextMessage(2, TimeUnit.SECONDS));

                //-- Verify final update with last Known BidOffer and lastPrice
                assertEquals("1,103.0,104.0,103.5", consumer.getNextMessage(2, TimeUnit.SECONDS));
                assertEquals(0, consumer.allMessageCount());
            }
        }
    }

    public void testBidAndOfferPublishedBeforeClientsConnect() throws Exception {
        bidOfferFeed.publish("1,103.0,104.0\n");
        bidOfferFeed.publish("2,104.0,105.0\n");
        bidOfferFeed.publish("3,105.0,106.0\n");
        lastPriceFeed.publish("1,103.5\n");
        lastPriceFeed.publish("2,104.5\n");
        lastPriceFeed.publish("3,106.5\n");

        try (
                JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE);
                JpInternalConsumer jpAlgo = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, "JP Algo");
                JpInternalConsumer jpPrimeServices = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, "JP Prime Services");
        ) {
            consumers.addAll(Arrays.asList(jpStride, jpAlgo, jpPrimeServices));
            waitTillAllConsumerAreConnected();


            for (JpInternalConsumer consumer : consumers) {
                consumer.awaitFirstMessage(2, TimeUnit.SECONDS);
                assertEquals("1,103.0,104.0,", consumer.getNextMessage(2, TimeUnit.SECONDS));

                assertEquals("2,104.0,105.0", consumer.getNextMessage(2, TimeUnit.SECONDS));
                assertEquals(0, consumer.allMessageCount());
            }
        }
    }


    private void waitTillAllConsumerAreConnected() throws TimeoutException, IOException, InterruptedException {
        for (JpInternalConsumer consumer : consumers) {
            consumer.connectToMarketHubAndListen();
            if (!consumer.awaitConnectionToMarketHub(1, TimeUnit.SECONDS)) {
                throw new TimeoutException("Connection to MarketHub failed");
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