package com.isharp.polozilla.components;

import com.isharp.polozilla.vo.CaptureWindowKey;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PoloCaptureRecordTimestampExtractor implements TimestampExtractor {
    public static final DateTimeFormatter fmt  = DateTimeFormatter.ofPattern("yyyy-MM-dd:HHmm");

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        String key = (String) record.key();
        LocalDateTime ld = LocalDateTime.parse(key,fmt);
        return ld.toInstant(ZoneOffset.UTC).toEpochMilli();
    }


}
