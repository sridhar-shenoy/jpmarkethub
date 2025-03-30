package com.jp.markethub.performance;

import com.jp.markethub.MarketHubTestBase;
import com.jp.markethub.mock.JpInternalConsumer;
import com.jp.markethub.util.TestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.jp.markethub.sanity.MarketHubSpecificationTest.JP_STRIDE;

public class MemoryProfile extends MarketHubTestBase {

    @Test
    public void memoryProfile() throws Exception {
        String intern = ",103.0,104.0;".intern();
        Runtime.getRuntime().gc();
        try (JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE)) {
            //-- Connect to Market Hub
            jpStride.connectToMarketHubAndListen();
            //-- Wait till it connects
            jpStride.awaitConnectionToMarketHub(1, TimeUnit.SECONDS);

            //-- Bloomberg now publishes data
            int count = 100000;
            List<Long> metrics = new ArrayList<>();
            long before = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            for (int i = 0; i < count; i++) {
                bidOfferFeed.publish(intern);
                TimeUnit.MICROSECONDS.sleep(10);
                if (i % 1000 == 0) {
                    long after = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                    long used = (before- after) / (1024 * 1024);
                    before = after;
                    System.out.println("Memory = " + used + " MB for i = [ " + i + " ] ");
                    metrics.add(used);
                }

                int finalI = i;
                TestUtils.waitTillTrue(() -> jpStride.getTotalCount() >
                        finalI, 2, TimeUnit.SECONDS);

            }
            double result = (double) metrics.stream()
                    .mapToLong(Long::longValue)
                    .sum()
                    / metrics.size();

            System.out.println("Average memory used : " + result);

        }
    }

}
