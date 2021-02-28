package com.isharp.polozilla.components;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.streams.processor.TimestampExtractor;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RawRecordTImestampExtractor implements TimestampExtractor {
    private static final Pattern timePattern = Pattern.compile("([\\d]{4})-([\\d]{2})-([\\d]{2})T([\\d]{2}):([\\d]{2}):([\\d]{2})\\.([\\d]{6}) (UTC)");
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Optional<Instant> fromRecord = timestampFrom((String)record.value());
        if (fromRecord.isPresent()) return fromRecord.get().toEpochMilli();
        return partitionTime;
    }

    public static Optional<Instant> timestampFrom(String str){
        Matcher m = timePattern.matcher(str);
        final boolean found = m.find();
        if (!found) return Optional.empty();
        int year = Integer.parseInt(m.group(1));
        int month =Integer.parseInt(m.group(2));
        int date = Integer.parseInt(m.group(3));

        int hour = Integer.parseInt(m.group(4));
        int min = Integer.parseInt(m.group(5));
        int sec = Integer.parseInt(m.group(6));
        int fractSec = Integer.parseInt(m.group(7))*1000;
        LocalDateTime ld = LocalDateTime.of(year,month,date,hour,min,sec,fractSec);
        return Optional.of(ld.toInstant(ZoneOffset.UTC));
    }

//iso instant 2011-12-03T10:15:30Z



}
