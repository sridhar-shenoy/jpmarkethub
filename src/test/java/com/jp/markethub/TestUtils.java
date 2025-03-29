package com.jp.markethub;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

public class TestUtils {
    /**
     * Waits until the condition is true or throws a TimeoutException.
     * @param condition Condition to check
     * @param timeout Maximum time to wait
     * @param unit Time unit (e.g., TimeUnit.SECONDS)
     * @throws TimeoutException If the condition isn't met within the timeout
     */
    public static void waitTillTrue(
        BooleanSupplier condition, 
        long timeout, 
        TimeUnit unit
    ) throws TimeoutException {
        long endTime = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < endTime) {
            if (condition.getAsBoolean()) {
                return; // Condition met
            }
            try {
                Thread.sleep(100); // Polling interval (adjust as needed)
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new TimeoutException("Wait interrupted");
            }
        }
        throw new TimeoutException("Condition not met after " + timeout + " " + unit);
    }
}