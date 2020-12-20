package com.isharp.polozilla.topologies.window;

public class Config {
    private final String inputTopic;
    private final String windowedSecsOutputTopic ;
    private final long timeWindoMillis;

    public Config(String inputTopic, String windowedSecsOutputTopic, long timeWindoMillis) {
        this.inputTopic = inputTopic;
        this.windowedSecsOutputTopic = windowedSecsOutputTopic;
        this.timeWindoMillis = timeWindoMillis;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getWindowedSecsOutputTopic() {
        return windowedSecsOutputTopic;
    }

    public long getTimeWindoMillis() {
        return timeWindoMillis;
    }
}
