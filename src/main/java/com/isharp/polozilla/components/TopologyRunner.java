package com.isharp.polozilla.components;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.isharp.polozilla.topologies.window.Config;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Properties;

public class TopologyRunner {

    static Properties config = new Properties();
    static {
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "confluent:29092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    public static void run (Topology topology, String appName){
        Properties myConf =  (Properties) config.clone();
        myConf.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        final KafkaStreams streams = new KafkaStreams(topology, myConf);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }



}
