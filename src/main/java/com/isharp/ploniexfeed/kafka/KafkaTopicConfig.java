package com.isharp.ploniexfeed.kafka;

import com.isharp.polozilla.topologies.flatten.Config;
import com.isharp.polozilla.topologies.flatten.FlattenRoute;
import com.isharp.polozilla.topologies.snap.SnapRoute;
import com.isharp.polozilla.topologies.window.WindowRoute;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaTopicConfig {

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;

    @Value(value="${kafka.tickers.input}")
    private String tickersInTopic;

    @Value(value="${kafka.tickers.flattened}")
    private String flattenedTickersTopic;

    @Value(value="${kafka.tickers.windowed.mins}")
    private String topicWindowsMins;


    @Value(value="${kafka.tickers.windowed.secs}")
    private String topicWindowsSec;

    @Value(value="${kafka.tickers.1.min.snaps}")
    private String topic1MinSnaps;






    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic tickersIn() {
        return new NewTopic(tickersInTopic, 1, (short) 1);
    }

    @Bean
    public NewTopic tickersWindwedSec() {
        return new NewTopic(topicWindowsSec, 1, (short) 1);
    }

    @Bean
    public NewTopic tickersWindwedMins() {
        return new NewTopic(topicWindowsMins, 1, (short) 1);
    }

    @Bean
    public NewTopic tickers1minSnaps() {
        return new NewTopic(topic1MinSnaps, 1, (short) 1);
    }

    @Bean
    public NewTopic tickersFlattened() {
        return new NewTopic(flattenedTickersTopic, 1, (short) 1);
    }
    @Bean("flatten_topology")
    public Topology tickersFlattenStream(){
        FlattenRoute flattenRoute = new FlattenRoute();
        com.isharp.polozilla.topologies.flatten.Config cfg = new Config("pol.tickers","pol.tickers.flattened");
        Topology flatten = flattenRoute.build(cfg);
        return flatten;
    }
    @Bean("tickers_window_secs")
    public Topology windowStream(){
        com.isharp.polozilla.topologies.window.Config winCfg = new com.isharp.polozilla.topologies.window.Config(
                "pol.tickers.flattened",
                "pol.tickers.windowed.secs", Duration.ofSeconds(1), Duration.ofSeconds(1),"pol.tickers.windowed.by.secs.aggregation");
        WindowRoute winRoute= new WindowRoute();
        Topology windowed = winRoute.build(winCfg);
        return windowed;
    }

    @Bean("tickers_window_mins")
    public Topology windowStreamMins(){
        com.isharp.polozilla.topologies.window.Config winCfg = new com.isharp.polozilla.topologies.window.Config(
                "pol.tickers.flattened",
                "pol.tickers.windowed.mins", Duration.ofMinutes(1), Duration.ofMinutes(1),"pol.tickers.windowed.by.mins.aggregation");
        WindowRoute winRoute= new WindowRoute();
        Topology windowed = winRoute.build(winCfg);
        return windowed;
    }

    @Bean("tickers_mins_snaps")
    public Topology mins_snaps(){
        com.isharp.polozilla.topologies.snap.Config snapCfg = new com.isharp.polozilla.topologies.snap.Config("pol.tickers.windowed.mins","pol.tickers.1.min.snaps",Duration.ofMinutes(1),Duration.ofMinutes(1));
        SnapRoute snapRoute= new SnapRoute();
        Topology windowed = snapRoute.build(snapCfg);
        return windowed;
    }

}