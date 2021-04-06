package com.isharp.polozilla.topologies.flatten;

import com.isharp.polozilla.components.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class FlattenRoute {
    public Topology build(Config cfg) {
        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(cfg.getInputTopic(),
                        Consumed.with(Serdes.String(), Serdes.String())
                                .withTimestampExtractor(new RawRecordTImestampExtractor())
                        )

                .filter((k,v)->(v.length()>50))
                .transform(new RawDataTransformerSupplier())

                .filter((k,v)->v.getMsgType()==1002)
                .flatTransform(new FlatCaptureTransformerSupplier())
                .to(cfg.getFlattenedOutputTopic(), Produced.with(Serdes.String(), PoloniexSerdes.capture));

        return builder.build();
    }

    public static void main(String[] args) {
        FlattenRoute route = new FlattenRoute();
        Config cfg = new Config("pol.tickers","pol.tickers.flattened");
        TopologyRunner.run(route.build(cfg),"flattenroutetopology");
    }
}
