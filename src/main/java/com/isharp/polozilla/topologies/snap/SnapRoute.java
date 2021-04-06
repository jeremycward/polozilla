package com.isharp.polozilla.topologies.snap;


import com.isharp.polozilla.components.PoloCaptureRecordTimestampExtractor;
import com.isharp.polozilla.components.PoloniexSerdes;
import com.isharp.polozilla.components.TopologyRunner;
import com.isharp.polozilla.vo.KeyedPoloCaptureWindow;
import com.isharp.polozilla.vo.Snap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class SnapRoute {
    private KTable<Windowed<String>,Snap> aggregatedTable(KStream<String, KeyedPoloCaptureWindow> inputStream,Config cfg){
        TimeWindows tumblingWindow = TimeWindows.of(cfg.getTimeWindow()).grace(cfg.getGracePeriod());
        KTable<Windowed<String>,Snap> snapWindows = inputStream
                .groupByKey()
                .windowedBy(tumblingWindow)
                .aggregate(
                        () -> new Snap(),
                        (key, value, aggregate) ->aggregate.add(value.getTicker(),value.getCaptureWindow()),
                        Materialized.with(Serdes.String(),PoloniexSerdes.snap))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        return snapWindows;
    }

    public Topology build(Config cfg) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, KeyedPoloCaptureWindow> keyedCaptures =
                builder.stream(cfg.getInputTopic(),
                                Consumed.with(Serdes.String(), PoloniexSerdes.keyedPoloCaptureWindow)
                                .withTimestampExtractor(new PoloCaptureRecordTimestampExtractor())
                );

        KTable<Windowed<String>,Snap> snapWindows = aggregatedTable(keyedCaptures,cfg);
        snapWindows.toStream()
                .to(cfg.getOutputTopic());
        return builder.build();
    }

    public static void main(String[] args) {
        SnapRoute route = new SnapRoute();
        com.isharp.polozilla.topologies.snap.Config cfg =
                new com.isharp.polozilla.topologies.snap.Config("pol.tickers.windowed.mins",
                        "pol.tickers.1.min.snaps",
                        Duration.ofMinutes(1),
                        Duration.ofMinutes(1));

        TopologyRunner.run(route.build(cfg),"snaproutetopology");


    }


}
