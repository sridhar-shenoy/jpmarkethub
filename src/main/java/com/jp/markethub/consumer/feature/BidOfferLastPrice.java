package com.jp.markethub.consumer.feature;

import com.jp.markethub.MarketHub;
import com.jp.markethub.log.Logger;
import com.jp.markethub.producer.ProducerType;
import com.jp.markethub.transport.TransportContract;

import java.util.Arrays;
import java.util.EnumSet;

public class BidOfferLastPrice implements FeatureContract {
    private final Logger logger = Logger.getInstance();
    private final TransportContract transportContract;
    private  String lastBid = "";
    private  String lastOffer = "";
    private  String lastPrice = "";
    private int sequence = 0;

    public BidOfferLastPrice(MarketHub hub, TransportContract transportContract) {
        this.transportContract = transportContract;
    }


    @Override
    public void onUpdate(byte[] data, int length, ProducerType type) {
        String newData = new String(data).trim();
        if (type == ProducerType.BIDOFFER) {
            String[] split = newData.split(",");
            if(split.length != 3){
                logger.error(getClass(),"Invalid Message [ " + newData + " ]");
                return;
            }
            lastBid = split[1];
            lastOffer = split[2];
        } else if (type == ProducerType.LASTPRICE) {
            String[] split = newData.split(",");
            lastPrice = split[1];
        }
        String combined = String.format("%d,%s,%s,%s\n", sequence++, lastBid, lastOffer, lastPrice);
        transportContract.publish(combined.getBytes(), combined.length());
        Arrays.fill(data, (byte) 0);
    }

    @Override
    public EnumSet<ProducerType> getInterestList() {
        return EnumSet.of(ProducerType.BIDOFFER, ProducerType.LASTPRICE);
    }

}
