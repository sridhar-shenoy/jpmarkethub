package com.jp.markethub;

import java.util.Arrays;
import java.util.EnumSet;

public class BidOfferLastPrice extends AbstractConsumer {
    private  String lastBid = "";
    private  String lastOffer = "";
    private  String lastPrice = "";
    private int sequence = 0;

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
            String[] split = newData.split(",");
            lastBid = split[1];
            lastOffer = split[2];
        } else if (type == ProducerType.LASTPRICE) {
            String[] split = newData.split(",");
            lastPrice = split[1];
        }
        String combined = String.format("%d,%s,%s,%s\n", sequence++, lastBid, lastOffer, lastPrice);
        publish(combined.getBytes(), combined.length());
        Arrays.fill(data, (byte) 0);
    }
}
