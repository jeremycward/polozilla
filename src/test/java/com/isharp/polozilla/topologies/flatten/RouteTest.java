package com.isharp.polozilla.topologies.flatten;

import com.isharp.polozilla.components.PoloniexSerdes;
import com.isharp.polozilla.vo.Capture;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.IsCloseTo.closeTo;

public class RouteTest {
    private static final String INPUT_TOPIC= RandomStringUtils.randomAlphabetic(15);
    private static final String OUTPUT_TOPIC= RandomStringUtils.randomAlphabetic(15);

    private Properties mockProps=null;

    private List<String> raw_inputRecords;
    private final Config testConfig = new Config(INPUT_TOPIC,OUTPUT_TOPIC);
    TestInputTopic<String,String> inputTopic;
    TestOutputTopic<String, Capture> outputTopic;
    List<TestRecord<String,String>> inputRecords;

    @Before
    public void setup()throws Exception{
        AtomicInteger  atomicInteger = new AtomicInteger(0);

        InputStream str = getClass().getResourceAsStream("/poloniex/tickers.txt");
        raw_inputRecords =IOUtils.readLines(new InputStreamReader(str));
         inputRecords = raw_inputRecords.stream().map((inputValue)->{
            return new TestRecord<String,String>(Integer.toString(atomicInteger.incrementAndGet()),inputValue);
        }).collect(Collectors.toList());


        mockProps = new Properties();
        mockProps.put("application.id", "polo-tick");
        mockProps.put("bootstrap.servers", "DUMMY_KAFKA_CONFLUENT_CLOUD_9092");
        mockProps.put("default.topic.replication.factor", "1");
        mockProps.put("offset.reset.policy", "latest");
        mockProps.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        Route flattenRoute = new Route();
        Topology underTest = flattenRoute.build(testConfig);
        TopologyTestDriver driver = new TopologyTestDriver(underTest,mockProps);
        inputTopic =
                driver.createInputTopic(testConfig.getInputTopic(), Serdes.String().serializer(),Serdes.String().serializer());

        outputTopic = driver.createOutputTopic(testConfig.getFlattenedOutputTopic(), Serdes.String().deserializer(), PoloniexSerdes.capture.deserializer()) ;
    }
    @Test
    public void shouldSerializeAndFlattenOneRec()throws Exception{
        inputTopic.pipeInput("33",raw_inputRecords.get(0));
        List<KeyValue<String,Capture>> values= outputTopic.readKeyValuesToList();
        assertThat(values.size(),is(2));
    }

    @Test
    public void shouldSerializeAndFlattenAll()throws Exception{

        inputRecords.forEach(it->inputTopic.pipeInput(it));
        List<KeyValue<String,Capture>> values= outputTopic.readKeyValuesToList();
        assertThat(values.size(),is(inputRecords.size()+1));
        assertThat(values.get(0).key,equalTo("125"));
        assertThat(values.get(0).value.getValue(),is(closeTo(0.10200002,0.4)));
        assertThat(values.get(88).value.getValue(),is(closeTo(6.03172715,0.4)));
        assertThat(values.get(1).key,equalTo("126"));
        assertThat(values.get(384).key,equalTo("356"));
        assertThat(values.get(382).key,equalTo("334"));
    }

}