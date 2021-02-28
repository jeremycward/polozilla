package com.isharp.polozilla.components;

import com.isharp.polozilla.vo.*;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;


public class KeyedPoloCaptureWindowTransformerSupplier implements TransformerSupplier<Windowed<String>, PoloCaptureWindow, KeyValue<String, KeyedPoloCaptureWindow>> {


    private final Transformer<Windowed<String>, PoloCaptureWindow,KeyValue<String, KeyedPoloCaptureWindow>> transformer = new Transformer<>(){
        @Override
        public void init(ProcessorContext context) {

        }

        @Override
        public KeyValue<String, KeyedPoloCaptureWindow> transform(Windowed<String> key, PoloCaptureWindow value) {
            Instant endTime = key.window().endTime();
            LocalDateTime dt = LocalDateTime.ofInstant(endTime, ZoneId.of("UTC"));
            String timeStamp = dt.format(PoloCaptureRecordTimestampExtractor.fmt);
            String acces = key.key();
            return KeyValue.pair(timeStamp,new KeyedPoloCaptureWindow(acces,value));
        }

        @Override
        public void close() {

        }
    };

    @Override
    public Transformer<Windowed<String>, PoloCaptureWindow, KeyValue<String, KeyedPoloCaptureWindow>> get() {
        return transformer;
    }
}
