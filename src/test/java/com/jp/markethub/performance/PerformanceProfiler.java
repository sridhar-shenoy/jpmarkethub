package com.jp.markethub.performance;

import com.jp.markethub.MarketHub;
import com.jp.markethub.mock.Bloomberg;
import com.jp.markethub.mock.JpInternalConsumer;
import com.jp.markethub.producer.ProducerType;
import com.jp.markethub.util.TestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.jp.markethub.MarketHubTestBase.BID_OFFER_LAST_PRICE_INTERNAL_PORT;
import static com.jp.markethub.sanity.MarketHubSpecificationTest.JP_STRIDE;
import static com.jp.markethub.util.TestUtils.waitTillTrue;
import static org.junit.Assert.assertEquals;

public class PerformanceProfiler {

    protected MarketHub hub;
    protected Bloomberg bidOfferFeed;
    protected Bloomberg lastPriceFeed;
    protected List<JpInternalConsumer> consumers;

    @Test
    public void profiler() throws TimeoutException, IOException, InterruptedException {
        extracted();
        extracted();
    }

    private void extracted() throws TimeoutException, IOException, InterruptedException {
        Thread thread = new Thread(this::startMarketHub, "setup");
        thread.start();
        consumers = new ArrayList<>();
        waitTillTrue(() -> bidOfferFeed != null, 1, TimeUnit.SECONDS);
        waitTillTrue(() -> lastPriceFeed != null, 1, TimeUnit.SECONDS);

        try (JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE)) {
            //-- Connect to Market Hub
            jpStride.connectToMarketHubAndListen();
            //-- Wait till it connects
            jpStride.awaitConnectionToMarketHub(1, TimeUnit.SECONDS);

            //-- Bloomberg now publishes data
            int count = 500;
            for (int i = 0; i < count; i++) {
                bidOfferFeed.publish(i + ",103.0,104.0;");
                TimeUnit.MICROSECONDS.sleep(10);
            }

            //-- Verify that Client has the data
            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);

            TestUtils.waitTillTrue(() -> jpStride.getTotalCount() > count - 1, 15, TimeUnit.SECONDS);
            assertEquals(count, jpStride.getTotalCount());
        }

        consumers.forEach(JpInternalConsumer::close);
        hub.stop();
        bidOfferFeed.stop();
        lastPriceFeed.stop();
        hub.reset();
        Thread.sleep(200);
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

}
