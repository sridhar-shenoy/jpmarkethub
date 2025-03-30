package com.jp.markethub.performance;

import com.jp.markethub.MarketHubTestBase;
import com.jp.markethub.mock.JpInternalConsumer;
import com.jp.markethub.util.TestUtils;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.jp.markethub.sanity.MarketHubSpecificationTest.JP_STRIDE;
import static org.junit.Assert.assertEquals;

public class PerformanceTest extends MarketHubTestBase {

    @Test
    public void publishLargeDataSetAtSlowPaceAndConsumerMustReceiveAllData() throws Exception {
        try (JpInternalConsumer jpStride = new JpInternalConsumer(BID_OFFER_LAST_PRICE_INTERNAL_PORT, JP_STRIDE)) {
            //-- Connect to Market Hub
            jpStride.connectToMarketHubAndListen();
            //-- Wait till it connects
            jpStride.awaitConnectionToMarketHub(1, TimeUnit.SECONDS);

            //-- Bloomberg now publishes data
            int count = 20000;
            for (int i = 0; i < count; i++) {
                bidOfferFeed.publish( i + ",103.0,104.0;");
                TimeUnit.MICROSECONDS.sleep(10);
            }

            //-- Verify that Client has the data
            jpStride.awaitFirstMessage(2, TimeUnit.SECONDS);

            TestUtils.waitTillTrue(()->jpStride.getTotalCount() > count-1, 15, TimeUnit.SECONDS);
            assertEquals(count, jpStride.getTotalCount());
        }
    }
}
