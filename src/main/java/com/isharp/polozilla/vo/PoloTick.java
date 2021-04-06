package com.isharp.polozilla.vo;

import com.google.gson.stream.JsonReader;

import java.io.IOException;

public class PoloTick {
    private Long ccyPairId;
    String lastTradePrie;
    String lowestAsk;
    String highestBid;
    String pctChange;
    String baseCcyVolume;
    String quoteCcyVolume;
    boolean isFrozen;

    String highestTradePrcie24hrs;
    String lowesstTradePrice24hrs;

    public Long getCcyPairId() {
        return ccyPairId;
    }

    public void setCcyPairId(Long ccyPairId) {
        this.ccyPairId = ccyPairId;
    }

    public String getLastTradePrie() {
        return lastTradePrie;
    }

    public void setLastTradePrie(String lastTradePrie) {
        this.lastTradePrie = lastTradePrie;
    }

    public String getLowestAsk() {
        return lowestAsk;
    }

    public void setLowestAsk(String lowestAsk) {
        this.lowestAsk = lowestAsk;
    }

    public String getHighestBid() {
        return highestBid;
    }

    public void setHighestBid(String highestBid) {
        this.highestBid = highestBid;
    }

    public String getPctChange() {
        return pctChange;
    }

    public void setPctChange(String pctChange) {
        this.pctChange = pctChange;
    }

    public String getBaseCcyVolume() {
        return baseCcyVolume;
    }

    public void setBaseCcyVolume(String baseCcyVolume) {
        this.baseCcyVolume = baseCcyVolume;
    }

    public String getQuoteCcyVolume() {
        return quoteCcyVolume;
    }

    public void setQuoteCcyVolume(String quoteCcyVolume) {
        this.quoteCcyVolume = quoteCcyVolume;
    }

    public boolean isFrozen() {
        return isFrozen;
    }

    public void setFrozen(boolean frozen) {
        isFrozen = frozen;
    }

    public String getHighestTradePrcie24hrs() {
        return highestTradePrcie24hrs;
    }

    public void setHighestTradePrcie24hrs(String highestTradePrcie24hrs) {
        this.highestTradePrcie24hrs = highestTradePrcie24hrs;
    }

    public String getLowesstTradePrice24hrs() {
        return lowesstTradePrice24hrs;
    }

    public void setLowesstTradePrice24hrs(String lowesstTradePrice24hrs) {
        this.lowesstTradePrice24hrs = lowesstTradePrice24hrs;
    }
    public static final  PoloTick fromReader(JsonReader rdr)throws IOException {
        PoloTick tick = new PoloTick();
        tick.setCcyPairId(rdr.nextLong());
        tick.setLastTradePrie(rdr.nextString());
        tick.setLowestAsk(rdr.nextString());
        tick.setHighestBid(rdr.nextString());
        tick.setPctChange(rdr.nextString());
        tick.setBaseCcyVolume(rdr.nextString());
        tick.setQuoteCcyVolume(rdr.nextString());
        tick.setFrozen(rdr.nextInt() > 0 ? true: false);
        tick.setHighestTradePrcie24hrs(rdr.nextString());
        tick.setLowesstTradePrice24hrs(rdr.nextString());
        rdr.nextInt();
        rdr.nextInt();
        return tick;
    }
}
