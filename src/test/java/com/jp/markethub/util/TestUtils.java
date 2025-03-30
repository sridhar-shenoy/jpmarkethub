package com.jp.markethub.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;

public class TestUtils {

    public static void waitTillTrueWithException(BooleanSupplier condition, long timeout, TimeUnit unit, boolean needException) throws TimeoutException {
        long endTime = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < endTime) {
            if (condition.getAsBoolean()) {
                return; // Condition met
            }
            try {
                Thread.sleep(100); // Polling interval (adjust as needed)
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (needException) {
                    throw new TimeoutException("Wait interrupted");
                }
            }
        }
        if (needException) {
            throw new TimeoutException("Condition not met after " + timeout + " " + unit);
        }
    }

    public static void waitTillTrue(BooleanSupplier condition, long timeout, TimeUnit unit) throws TimeoutException {
        waitTillTrueWithException(condition, timeout, unit, true);
    }

}