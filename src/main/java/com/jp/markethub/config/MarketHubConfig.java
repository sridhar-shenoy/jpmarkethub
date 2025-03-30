package com.jp.markethub.config;

public class MarketHubConfig {

    public int getBufferSize() {
        return (int) Math.pow(2, 14);
    }
}
