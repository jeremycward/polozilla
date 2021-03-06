package com.isharp.polozilla.topologies.window;

import java.time.Duration;

public class Config {
    private final String inputTopic;
    private final String outputTopic;
    private final Duration timeWindow;
    private final Duration gracePeriod;
    private final String aggregationName;

    public Config(String inputTopic, String windowedSecsOutputTopic, Duration timeWindow, Duration gracePeriod, String aggregationName) {
        this.inputTopic = inputTopic;
        this.outputTopic = windowedSecsOutputTopic;
        this.timeWindow = timeWindow;
        this.gracePeriod = gracePeriod;
        this.aggregationName = aggregationName;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public Duration getTimeWindow() {
        return timeWindow;
    }

    public Duration getGracePeriod() {
        return gracePeriod;
    }

    public String getAggregationName() {
        return aggregationName;
    }

    public String getOutputTopic() {
        return outputTopic;
    }




}
