package com.isharp.polozilla;

import com.isharp.polozilla.topologies.flatten.Config;
import com.isharp.polozilla.topologies.flatten.FlattenRoute;
import com.isharp.polozilla.topologies.window.WindowRoute;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.time.Duration;
import java.util.Properties;

public class PolStreamApp {

    public static void main(final String[] args) throws Exception {
        Properties config = new Properties();

        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "confluent:29092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        FlattenRoute flattenRoute = new FlattenRoute();
        com.isharp.polozilla.topologies.flatten.Config cfg = new Config("pol.tickers","pol.tickers.flattened");
        Topology flatten = flattenRoute.build(cfg);

        com.isharp.polozilla.topologies.window.Config winCfg = new com.isharp.polozilla.topologies.window.Config(
                "pol.tickers.flattened",
                "pol.tickers.windowed.secs", Duration.ofSeconds(1), Duration.ofSeconds(1),"pol.tickers.windowed.by.secs.aggregation");
        WindowRoute winRoute= new WindowRoute();
        Topology windowed = winRoute.build(winCfg);
        Properties flattenProps =(Properties) config.clone();
        Properties windowProps =(Properties) config.clone();
        flattenProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "POL.FLATTEN");
        windowProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "POL.WINDOWED");
        final KafkaStreams flattenStream = new KafkaStreams(flatten,flattenProps);
        flattenStream.start();

        final KafkaStreams winStream = new KafkaStreams(windowed,windowProps);
        winStream.start();


        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                flattenStream.close();
                winStream.close();
            }
        }));
    }

}