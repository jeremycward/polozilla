package com.isharp.polozilla.topologies.window;


import com.isharp.polozilla.components.KeyedPoloCaptureWindowTransformerSupplier;
import com.isharp.polozilla.components.PoloniexSerdes;
import com.isharp.polozilla.components.TopologyRunner;
import com.isharp.polozilla.vo.Capture;
import com.isharp.polozilla.vo.KeyedPoloCaptureWindow;
import com.isharp.polozilla.vo.PoloCaptureWindow;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
public class WindowRoute {

    public Topology build(Config cfg) {
        StreamsBuilder builder = new StreamsBuilder();
        Consumed<String, Capture> captureConsumerOptions = Consumed.with(Serdes.String(), PoloniexSerdes.capture);
        TransformerSupplier<Windowed<String>, PoloCaptureWindow, KeyValue<String, KeyedPoloCaptureWindow>> keyedTransformerSupplier =
        new KeyedPoloCaptureWindowTransformerSupplier();

        KStream<String, Capture> captureEvents =
                builder.stream(cfg.getInputTopic(), captureConsumerOptions);




        TimeWindows tumblingWindow = TimeWindows.of(cfg.getTimeWindow()).grace(cfg.getGracePeriod());

        KTable<Windowed<String>, PoloCaptureWindow> pulseWindows =
                captureEvents
                        .groupByKey()
                        .windowedBy(tumblingWindow)
                        .aggregate(PoloCaptureWindow::new,(k, v, win)->win.push(v)
                        ,Materialized.with(Serdes.String(),PoloniexSerdes.captureWindow))
                        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        pulseWindows
                .toStream()
                .transform(keyedTransformerSupplier)
                .to(cfg.getOutputTopic(),Produced.with(Serdes.String(),PoloniexSerdes.keyedPoloCaptureWindow));


        return builder.build();
    }

    public static void main(String[] args) {
        WindowRoute route = new WindowRoute();
        com.isharp.polozilla.topologies.window.Config cfg =
                new com.isharp.polozilla.topologies.window.Config("pol.tickers.flattened",
                        "pol.tickers.windowed.mins",
                        Duration.ofMinutes(1),
                        Duration.ofSeconds(5),
                        null);
        TopologyRunner.run(route.build(cfg),"windowroutetopology");


    }
}
