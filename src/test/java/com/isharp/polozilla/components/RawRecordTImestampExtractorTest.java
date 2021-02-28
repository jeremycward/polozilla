package com.isharp.polozilla.components;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.TemporalField;
import java.util.Optional;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class RawRecordTImestampExtractorTest {
    String FEB_20_RECORD = "2021-02-20T10:34:43.182059 UTC" + RandomStringUtils.randomAlphanumeric(1000);
    String RECORD_NO_TIMESTAMP = RandomStringUtils.randomAlphabetic(1000);
    @Test
    public void timestampFromRandomStringIsNull() {
        Optional<Instant> i = RawRecordTImestampExtractor.timestampFrom(RECORD_NO_TIMESTAMP);
        assertFalse(i.isPresent());
    }

    @Test
    public void timestampFromRecordIsExtracted() {
        Optional<Instant> i = RawRecordTImestampExtractor.timestampFrom(FEB_20_RECORD);
        assertTrue(i.isPresent());
        LocalDateTime ld_utc = LocalDateTime.ofInstant(i.get(), ZoneId.of("UTC"));
        assertThat(ld_utc.getYear(),is(2021));
        assertThat(ld_utc.getMonth().getValue(), is(2));
        assertThat(ld_utc.getDayOfMonth(),is(20));
        assertThat(ld_utc.getDayOfMonth(),is(20));
        assertThat(ld_utc.getHour(),is(10));
        assertThat(ld_utc.getMinute(),is(34));
        assertThat(ld_utc.getSecond(),is(43));
        assertThat(i.get().getNano(),is(182059000));

    }


}