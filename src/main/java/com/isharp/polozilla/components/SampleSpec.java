package com.isharp.polozilla.components;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

public class SampleSpec {
    private final List<String> assets;
    private final LocalTime start;
    private final LocalTime end;
    private final Duration interval;
    private final String aggregationId;

    public SampleSpec(List<String> assets, LocalTime start, LocalTime end, Duration interval) {
        this.assets = assets;
        this.start = start;
        this.end = end;
        this.interval = interval;
        this.aggregationId = RandomStringUtils.randomAlphabetic(50);
    }
    public boolean hasAsset(String asseet){
        return assets.contains(asseet);
    }

    public List<String> getAssets() {
        return assets;
    }
    public Pair<LocalDateTime,LocalDateTime>  getTimeRange(LocalDate localDate){
        return Pair.of(localDate.atStartOfDay().with(start),localDate.atStartOfDay().with(end));
    }

    public Duration getInterval() {
        return interval;
    }

    public String getAggregationId() {
        return aggregationId;
    }
}
