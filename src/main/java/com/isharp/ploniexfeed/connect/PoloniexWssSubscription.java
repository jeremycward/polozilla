package com.isharp.ploniexfeed.connect;

import com.google.gson.Gson;

public class PoloniexWssSubscription {
    public final static transient PoloniexWssSubscription TICKER = new PoloniexWssSubscription("1002");

    public final String command;
    public final String channel;

    public PoloniexWssSubscription(String channel) {
        this.command = "subscribe";
        this.channel = channel;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }


}
