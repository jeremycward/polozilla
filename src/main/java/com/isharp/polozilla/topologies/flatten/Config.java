package com.isharp.polozilla.topologies.flatten;

public class Config {
    private final String inputTopic;
    private final String flattenedOutputTopic;

    public Config(String rawTickersInput, String flattenedOutput) {
        this.inputTopic = rawTickersInput;
        this.flattenedOutputTopic = flattenedOutput;
    }

    public String getInputTopic() {
        return inputTopic;
    }

    public String getFlattenedOutputTopic() {
        return flattenedOutputTopic;
    }
}
