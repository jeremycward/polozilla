package com.isharp.polozilla.topologies.snap;

import java.time.Duration;

public class Config {
    private final String inputTopic;
    private final String outputTopic;
    private final Duration timeWindow;
    private final Duration gracePeriod;

    public Config(String inputTopic, String outputTopic, Duration timeWindow, Duration gracePeriod) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.timeWindow = timeWindow;
        this.gracePeriod = gracePeriod;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public Duration getTimeWindow() {
        return timeWindow;
    }

    public Duration getGracePeriod() {
        return gracePeriod;
    }
}
