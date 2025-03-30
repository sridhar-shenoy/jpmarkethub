package com.jp.markethub.performance;

import com.jp.markethub.MarketHub;
import com.jp.markethub.config.MarketHubConfig;
import com.jp.markethub.mock.Bloomberg;
import com.jp.markethub.mock.JpInternalConsumer;
import com.jp.markethub.producer.ProducerType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.jp.markethub.MarketHubTestBase.BID_OFFER_LAST_PRICE_INTERNAL_PORT;
import static com.jp.markethub.sanity.MarketHubSpecificationTest.JP_STRIDE;
import static com.jp.markethub.util.TestUtils.waitTillTrue;
import static com.jp.markethub.util.TestUtils.waitTillTrueWithException;

public class PerformanceProfiler {

    public static final String DATA = ",103.0,104.0;";
    protected MarketHub hub;
    protected Bloomberg bidOfferFeed;
    protected Bloomberg lastPriceFeed;
    protected List<JpInternalConsumer> consumers;

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        new PerformanceProfiler().start();
    }

    private void start() throws IOException, InterruptedException, TimeoutException {
        int count = 10000;
        Runtime.getRuntime().gc();
        for (int i = 1; i < 10; i++) {
            System.out.println("PERF-- Iteration [ " + i + " ] count = [" + count + " ] ");
            iteration(count);
        }
    }


    private void iteration(int count) throws TimeoutException, IOException, InterruptedException {
        Thread thread = new Thread(this::startMarketHub, "setup");
        thread.start();
        consumers = new ArrayList<>();
        waitTillTrue(() -> bidOfferFeed != null, 1, TimeUnit.SECONDS);

        try (JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE)) {
            //-- Connect to Market Hub
            jpStride.connectToMarketHubAndListen();
            //-- Wait till it connects
            jpStride.awaitConnectionToMarketHub(1, TimeUnit.SECONDS);

            long before = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            //-- Bloomberg now publishes data
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                bidOfferFeed.publish(DATA);
                TimeUnit.MICROSECONDS.sleep(2);
            }
            long t2 = System.currentTimeMillis();

            //-- Verify that Client has the data
            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);

            waitTillTrueWithException(() -> jpStride.getTotalCount() > count - 1, 15, TimeUnit.SECONDS, false);
            System.out.println("PERF -- DIFF[ " + (count - jpStride.getTotalCount()) + " ] latency [ " + Math.min(getElapsed() - (t2-t1),0) + " ms ] timeToPublishData [ " + (t2 - t1) + " ms ] ");
        }

        consumers.forEach(JpInternalConsumer::close);
        hub.stop();
        bidOfferFeed.stop();
        hub.reset();
        Thread.sleep(200);
    }

    private long getElapsed() {
        return System.currentTimeMillis() - hub.getFirstDataTime(ProducerType.BIDOFFER);
    }



    protected void startMarketHub() {
        hub = new MarketHub(new MarketHubConfig());
        try {
            hub.startConsumer();

            //-- Start Mock Bloomberg Feed
            bidOfferFeed = new Bloomberg(9000);
            bidOfferFeed.start();

            //-- Connect MarketHub to Mock Bloomberg
            hub.connectToProducer(ProducerType.BIDOFFER, 9000);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
