package com.isharp.polozilla.topologies.flatten;

import com.isharp.polozilla.components.FlatCaptureTransformerSupplier;
import com.isharp.polozilla.components.PoloniexSerdes;
import com.isharp.polozilla.components.RawDataTransformerSupplier;
import com.isharp.polozilla.vo.Capture;
import com.isharp.polozilla.vo.PoloWebsockMsg;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class Route {

    TimestampExtractor tsExtractor = (record, partitionTime) -> record.timestamp();
    public Topology build(Config cfg) {
        StreamsBuilder builder = new StreamsBuilder();

        builder
                .stream(cfg.getInputTopic(), Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(tsExtractor))
                .transform(new RawDataTransformerSupplier())
                .flatTransform(new FlatCaptureTransformerSupplier())
                .to(cfg.getFlattenedOutputTopic(), Produced.with(Serdes.String(), PoloniexSerdes.capture));

        return builder.build();
    }
}
