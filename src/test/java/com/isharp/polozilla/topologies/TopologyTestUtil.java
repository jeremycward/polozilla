package com.isharp.polozilla.topologies;

import com.isharp.polozilla.vo.Capture;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.Properties;


public class TopologyTestUtil {

    public static TopologyTestDriver driverFor(Topology top){
        Properties mockProps=null;
        mockProps = new Properties();
        mockProps.put("application.id", "polo-tick");
        mockProps.put("bootstrap.servers", "DUMMY_KAFKA_CONFLUENT_CLOUD_9092");
        mockProps.put("default.topic.replication.factor", "1");
        mockProps.put("offset.reset.policy", "latest");
        mockProps.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        return new TopologyTestDriver(top,mockProps);
    }
    public static class PriceSender{
        final Long startTime = System.currentTimeMillis();
        Long currentTime = startTime;
        final TestInputTopic<String, Capture> inputTopic;

        public PriceSender(TestInputTopic<String, Capture> inputTopic) {
            this.inputTopic = inputTopic;
        }

        public PriceSender sendAPrice(String ccy, double amount){
            Capture capture  = new Capture();
            capture.setTimeStamp(currentTime);
            capture.setValue(amount);
            inputTopic.pipeInput(ccy,capture);
            return this;
        }
        public PriceSender  advanceMillis(long millis){
            currentTime += millis;
            return this;
        }
    }

}
