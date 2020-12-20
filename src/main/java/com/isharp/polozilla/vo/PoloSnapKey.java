package com.isharp.polozilla.vo;

public class PoloSnapKey {

    private Long endTime;
    private  String ticker;

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public static PoloSnapKey of(String ticker,Long endTime){
        PoloSnapKey snapKey = new PoloSnapKey();
        snapKey.setTicker(ticker);
        snapKey.setEndTime(endTime);
        return snapKey;
        }

        }
