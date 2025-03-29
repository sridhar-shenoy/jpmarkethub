package com.jp.markethub;

import java.util.EnumSet;

public class BidOfferLastPrice extends AbstractConsumer {
    private volatile String lastBidOffer = "";
    private volatile String lastPrice = "";

    public BidOfferLastPrice(MarketHub hub) {
        super(hub, EnumSet.of(ProducerType.BIDOFFER, ProducerType.LASTPRICE));
    }

    @Override
    public void run() {
        super.run();
    }

    public void onUpdate(byte[] data, int length, ProducerType type) {
        String newData = new String(data).trim();
        if (type == ProducerType.BIDOFFER) {
            lastBidOffer = newData;
        } else if (type == ProducerType.LASTPRICE) {
            String[] split = newData.split(",");
            lastPrice = split[1];
        }
        String combined = String.format("%s,%s%n\n", lastBidOffer, lastPrice);
        String s = combined.trim() + "\n";
        publish(s.getBytes(), s.length());
    }
}
