package com.jp.markethub;

import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MarketHubTestBase {
    public static final int BID_OFFER_LAST_PRICE_INTERNAL_PORT = 10000;
    protected MarketHub hub;
    protected Bloomberg bidOfferFeed;
    protected Bloomberg lastPriceFeed;
    protected List<JpInternalConsumer> consumers;

    @Before
    public void setUp() throws TimeoutException {
        Thread thread = new Thread(this::startMarketHub, "setup");
        thread.start();
        consumers = new ArrayList<>();
        TestUtils.waitTillTrue(()->bidOfferFeed != null, 1, TimeUnit.SECONDS);
        TestUtils.waitTillTrue(()->lastPriceFeed != null, 1, TimeUnit.SECONDS);
    }

    protected void startMarketHub() {
        //-- Initialize MarketHub
        hub = new MarketHub();
        try {
            hub.startConsumer();

            //-- Start Mock Bloomberg Feed
            bidOfferFeed = new Bloomberg(9000);
            lastPriceFeed = new Bloomberg(9001);
            bidOfferFeed.start();
            lastPriceFeed.start();

            //-- Connect MarketHub to Mock Bloomberg
            hub.connectToProducer(ProducerType.BIDOFFER, 9000);
            hub.connectToProducer(ProducerType.LASTPRICE, 9001);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void tearDown() throws Exception {
        consumers.forEach(JpInternalConsumer::close);
        hub.stop();
        bidOfferFeed.stop();
        lastPriceFeed.stop();
        hub.reset();
        Thread.sleep(200);
    }
}
