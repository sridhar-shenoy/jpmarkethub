package com.jp.markethub.sanity;

import com.jp.markethub.MarketHubTestBase;
import com.jp.markethub.mock.JpInternalConsumer;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.jp.markethub.util.TestUtils.waitTillTrue;
import static org.junit.Assert.assertEquals;

public class MarketHubSpecificationTest extends MarketHubTestBase {

    public static final String JP_STRIDE = "JP Stride";
    public static final String JP_ALGO = "JP Algo";
    public static final String JP_PRIME_SERVICES = "JP Prime Services";

    @Test
    public void singleConsumerAndSingleProducer() throws Exception {
        try (JpInternalConsumer jpAlgo = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_ALGO)) {

            //-- Connect to Market Hub
            jpAlgo.connectToMarketHubAndListen();

            //-- Wait till it connects
            jpAlgo.awaitConnectionToMarketHub(1, TimeUnit.SECONDS);

            //-- Bloomberg now publishes data
            bidOfferFeed.publish("1,103.0,104.0;");

            //-- Verify that Client has the data
            jpAlgo.awaitFirstMessage(2, TimeUnit.SECONDS);
            String message = jpAlgo.getNextMessage(1, TimeUnit.SECONDS);
            assertEquals("0,103.0,104.0,", message);

            //-- Ensure there are no messages left to consume
            assertEquals(0, jpAlgo.allMessageCount());
        }
    }

    @Test
    public void singleConsumerMustGetCollatedDataFromMultipleProducers() throws Exception {
        try (JpInternalConsumer jpAlgo = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_ALGO)) {

            //-- Connect Internal JP Client to MarketHub for BifOffer and Last Price
            jpAlgo.connectToMarketHubAndListen();

            //-- Wait till it connects
            jpAlgo.awaitConnectionToMarketHub(1, TimeUnit.SECONDS);

            //-- Bloomberg now publishes data
            bidOfferFeed.publish("1,103.0,104.0;");
            lastPriceFeed.publish("1,103.5;");

            //-- Verify initial bid/offer
            jpAlgo.awaitFirstMessage(1, TimeUnit.SECONDS);
            assertEquals("0,103.0,104.0,", jpAlgo.getNextMessage(2, TimeUnit.SECONDS));

            //-- Verify combined update
            waitTillTrue(() -> jpAlgo.allMessageCount() > 0, 1, TimeUnit.SECONDS);
            assertEquals("1,103.0,104.0,103.5", jpAlgo.getNextMessage(2, TimeUnit.SECONDS));

            //-- Disconnect BidOffer feed
            bidOfferFeed.stop();

            //-- Send new last price
            lastPriceFeed.publish("2,104.0;");

            //-- Verify final update with last Known BidOffer and lastPrice
            waitTillTrue(() -> jpAlgo.allMessageCount() > 0, 500, TimeUnit.SECONDS);
            String result = jpAlgo.getNextMessage(1, TimeUnit.SECONDS);
            assertEquals("2,103.0,104.0,104.0", result);

            //-- Ensure there are no messages left to consume
            assertEquals(0, jpAlgo.allMessageCount());
        }
    }

    @Test
    public void multipleConsumersJoinedBeforeProducerMustReceiveAllDataFromMultipleProducers() throws Exception {
        try (
                JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE);
                JpInternalConsumer jpAlgo = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_ALGO);
                JpInternalConsumer jpPrimeServices = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_PRIME_SERVICES);
        ) {
            consumers.addAll(Arrays.asList(jpStride, jpAlgo, jpPrimeServices));
            waitTillAllConsumerAreConnected();

            bidOfferFeed.publish("1,103.0,104.0;");
            lastPriceFeed.publish("1,103.5;");

            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);
            assertEquals("0,103.0,104.0,", jpStride.getNextMessage(2, TimeUnit.SECONDS));

            waitTillTrue(() -> jpStride.allMessageCount() > 0, 200, TimeUnit.SECONDS);
            assertEquals("1,103.0,104.0,103.5", jpStride.getNextMessage(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void multipleConsumersJoinedLaterThanAllProducerEnsuresFirstConsumerToReceiveAllData() throws Exception {
        bidOfferFeed.publish("1,103.0,104.0;");
        bidOfferFeed.publish("2,104.0,105.0;");
        bidOfferFeed.publish("3,105.0,106.0;");

        try (
                JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE);
                JpInternalConsumer jpAlgo = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_ALGO);
                JpInternalConsumer jpPrimeServices = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_PRIME_SERVICES);
        ) {
            consumers.addAll(Arrays.asList(jpStride, jpAlgo, jpPrimeServices));
            waitTillAllConsumerAreConnected();



            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);
            assertEquals("0,103.0,104.0,", jpStride.getNextMessage(2, TimeUnit.SECONDS));

            waitTillTrue(() -> jpStride.allMessageCount() > 0, 200, TimeUnit.SECONDS);
            assertEquals("1,104.0,105.0,", jpStride.getNextMessage(2, TimeUnit.SECONDS));

            waitTillTrue(() -> jpStride.allMessageCount() > 0, 2, TimeUnit.SECONDS);
            assertEquals("2,105.0,106.0,", jpStride.getNextMessage(2, TimeUnit.SECONDS));

            /*
                2nd and 3rd client may only get updates from the time of connection
             */
        }
    }
}