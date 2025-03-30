package com.jp.markethub.performance;

import com.jp.markethub.MarketHub;
import com.jp.markethub.config.MarketHubConfig;
import com.jp.markethub.mock.Bloomberg;
import com.jp.markethub.mock.JpInternalConsumer;
import com.jp.markethub.producer.ProducerType;
import org.junit.Test;

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
        int count = 20000;
        for (int i = 1; i < 10; i++) {
            int dataToPublish = count * i;
            int timeout = 10 - i;
            System.out.println("PERF-- Iteration [ " + i  +" ] count = [" + dataToPublish +" ] timeout [ " + timeout +" ] ");
            iteration(dataToPublish, timeout);
        }
    }



    private void iteration(int count, int timeout) throws TimeoutException, IOException, InterruptedException {
        Thread thread = new Thread(this::startMarketHub, "setup");
        thread.start();
        consumers = new ArrayList<>();
        waitTillTrue(() -> bidOfferFeed != null, 1, TimeUnit.SECONDS);

        try (JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE)) {
            //-- Connect to Market Hub
            jpStride.connectToMarketHubAndListen();
            //-- Wait till it connects
            jpStride.awaitConnectionToMarketHub(1, TimeUnit.SECONDS);

            MemoryStats startMemory = captureMemoryStats();
            //-- Bloomberg now publishes data
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                bidOfferFeed.publish(DATA);
                TimeUnit.MICROSECONDS.sleep(timeout);
            }
            long t2 = System.currentTimeMillis();

            //-- Verify that Client has the data
            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);

            waitTillTrueWithException(() -> jpStride.getTotalCount() > count - 1, 15, TimeUnit.SECONDS, false);
            System.out.println("PERF -- DIFF[ " + (count - jpStride.getTotalCount()) + " ] elapse [ " + (System.currentTimeMillis() -hub.getFirstDataTime(ProducerType.BIDOFFER)) +" ms ] timeToPublishData [ " + (t2 - t1 ) + " ms ] ");
            MemoryStats endMemory = captureMemoryStats();
            System.out.println(
                    "PERF -- MEMORY [Start] " + startMemory + "\n" +
                            "        [After Publish] " + endMemory + " (Δ+" +
                            (endMemory.used() - startMemory.used())/1024 + " KB)\n" +
                            "        [Final] " + endMemory + " (Δ+" +
                            (endMemory.used() - startMemory.used())/1024 + " KB)"
            );
        }

        consumers.forEach(JpInternalConsumer::close);
        hub.stop();
        bidOfferFeed.stop();
        lastPriceFeed.stop();
        hub.reset();
        Thread.sleep(200);
    }

    private MemoryStats captureMemoryStats() {
        Runtime runtime = Runtime.getRuntime();
        runtime.gc(); // Suggest GC run (not guaranteed)
        return new MemoryStats(
                runtime.totalMemory(),
                runtime.freeMemory(),
                runtime.maxMemory()
        );
    }

    private static class MemoryStats {
        final long total;
        final long free;
        final long max;

        public MemoryStats(long total, long free, long max) {
            this.total = total;
            this.free = free;
            this.max = max;
        }

        public long used() {
            return total - free;
        }

        public String toString() {
            return String.format(
                    "Used: %d MB, Total: %d MB, Max: %d MB",
                    used() / (1024 * 1024),
                    total / (1024 * 1024),
                    max / (1024 * 1024)
            );
        }
    }

    protected void startMarketHub() {
        //-- Initialize MarketHub
        hub = new MarketHub( new MarketHubConfig());
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
