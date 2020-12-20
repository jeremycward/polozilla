package com.isharp.polozilla.topologies.window;


import com.isharp.polozilla.components.PoloniexSerdes;
import com.isharp.polozilla.vo.Capture;
import com.isharp.polozilla.vo.PoloCaptureWindow;
import com.isharp.polozilla.vo.PoloSnapKey;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Date;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class Route {
    public Topology build(Config cfg) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Capture> source = builder.stream(cfg.getInputTopic(), Consumed.with(Serdes.String(), PoloniexSerdes.capture).withTimestampExtractor((record, partitionTime) -> ((Capture) record.value()).getTimeStamp()));
        TimeWindowedKStream<String, Capture> windowed =
                source.groupByKey()
                        .windowedBy(TimeWindows.of(Duration.ofMillis(cfg.getTimeWindoMillis()))
                                .advanceBy(Duration.ofMillis(cfg.getTimeWindoMillis())).grace(Duration.ofMillis(0)));

        KTable<Windowed<String>, PoloCaptureWindow> aggregated = windowed
                .aggregate(PoloCaptureWindow::new, (k, v, agg) -> agg.push(v),Named.as("time-window-aggregation") ,Materialized.with(Serdes.String(), PoloniexSerdes.captureWindow));

        KStream<PoloSnapKey, PoloCaptureWindow> streamed = aggregated
                .suppress(Suppressed.untilWindowCloses(unbounded()))
                .toStream(
                        (k, v) -> {
                            String ticker = k.key();
                            Long endTime = k.window().end();
                            PoloSnapKey snapKey = PoloSnapKey.of(ticker, endTime);
                            return new KeyValue<>(snapKey, v);
                        }

                ).selectKey((k,v)->k.key);

        streamed.
//                peek((k,v)->{
//                            PoloSnapKey snapkey =(PoloSnapKey)k;
//                            System.out.println(".................................." + snapkey);
//                            System.out.println(".................................." + v);
//                            System.out.println(snapkey.getTicker() + new Date(snapkey.getEndTime()));
//                            System.out.println(v.getCaptures().size());
//                            System.out.println("..................................");
//
//                        }
//                ).

                to(cfg.getWindowedSecsOutputTopic(),Produced.with(PoloniexSerdes.snapKey,PoloniexSerdes.captureWindow));
        return builder.build();
    }


}
