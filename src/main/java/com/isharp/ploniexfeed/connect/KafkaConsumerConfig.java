package com.isharp.ploniexfeed.connect;



import java.util.HashMap;
import java.util.Map;

import com.isharp.polozilla.components.PoloniexSerdes;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    @Autowired  KafkaAdmin kafkaAdmin;
    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    public Map<String, Object> consumerConfig(){

        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.putAll(kafkaAdmin.getConfigurationProperties());
        consumerConfig.put("group.id","snap_consumer");
        consumerConfig.put("key.deserializer" ,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return consumerConfig;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(){
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        DefaultKafkaConsumerFactory defaultKafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig());
        defaultKafkaConsumerFactory.setKeyDeserializer(WindowedSerdes.timeWindowedSerdeFrom(String.class).deserializer());
        defaultKafkaConsumerFactory.setValueDeserializer(PoloniexSerdes.snap.deserializer());
        factory.setConsumerFactory(defaultKafkaConsumerFactory);
        return factory;
    }
}
