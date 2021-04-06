package com.isharp.polozilla.topologies.flatten;


import com.isharp.polozilla.components.PoloniexSerdes;
import com.isharp.polozilla.topologies.TopologyTestUtil;
import com.isharp.polozilla.vo.Capture;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.isharp.polozilla.topologies.TopologyTestUtil.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.number.IsCloseTo.closeTo;

public class FlattenRouteTest {
    private static final String INPUT_TOPIC= RandomStringUtils.randomAlphabetic(15);
    private static final String OUTPUT_TOPIC= RandomStringUtils.randomAlphabetic(15);

    private Properties mockProps=null;

    private List<String> raw_inputRecords;
    private final Config testConfig = new Config(INPUT_TOPIC,OUTPUT_TOPIC);
    TestInputTopic<String,String> inputTopic;
    TestOutputTopic<String, Capture> outputTopic;
    List<TestRecord<String,String>> inputRecords;
    TopologyTestDriver driver;

    final Logger logger = LoggerFactory.getLogger(FlattenRouteTest.class);
    @Before
    public void setup()throws Exception{
        AtomicInteger  atomicInteger = new AtomicInteger(0);

        InputStream str = getClass().getResourceAsStream("/poloniex/tickers.txt");
        Instant currentInstant = TopologyTestUtil.recordBaseTime;
        AtomicInteger recCount = new AtomicInteger(0);
        raw_inputRecords = IOUtils.readLines(new InputStreamReader(str));
         inputRecords = raw_inputRecords.stream()
                 .map((inputValue)->
                         new TestRecord<String,String>(
                                 Integer.toString(atomicInteger.incrementAndGet()),
                                 inputValue,
                                 recordBaseTime.plusMillis(recCount.incrementAndGet())
                         ) )
        .collect(Collectors.toList());

        mockProps = new Properties();
        mockProps.put("application.id", "polo-tick");
        mockProps.put("bootstrap.servers", "DUMMY_KAFKA_CONFLUENT_CLOUD_9092");
        mockProps.put("default.topic.replication.factor", "1");
        mockProps.put("offset.reset.policy", "latest");
        //mockProps.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        FlattenRoute flattenRoute = new FlattenRoute();
        Topology underTest = flattenRoute.build(testConfig);
         driver = new TopologyTestDriver(underTest,mockProps, FEB_2_2021_17_30_Instant);


        inputTopic =
                driver.createInputTopic(testConfig.getInputTopic(), Serdes.String().serializer(),Serdes.String().serializer(),recordBaseTime,advance1Milli);

        outputTopic = driver.createOutputTopic(testConfig.getFlattenedOutputTopic(), Serdes.String().deserializer(), PoloniexSerdes.capture.deserializer()) ;
    }
    @Test
    public void shouldSerializeAndFlattenOneRec()throws Exception{

        inputTopic.pipeInput("33",raw_inputRecords.get(0),recordBaseTime);

        List<KeyValue<String,Capture>> values= outputTopic.readKeyValuesToList();
        assertThat(values.size(),is(2));
        logger.info("{}",new Date(values.get(0).value.getTimeStamp()));
        assertThat(values.get(0).value.getTimeStamp(),equalTo(FEB_2_2021_17_30_Instant.toEpochMilli()));
    }

    @Test
    public void shouldSerializeAndFlattenAll()throws Exception{
        inputRecords.forEach(it->
                {
                    inputTopic.pipeInput(it);
                }

        );
        List<KeyValue<String,Capture>> values= outputTopic.readKeyValuesToList();
        assertThat(values.size(),is(inputRecords.size()+1));
        assertThat(values.get(0).key,equalTo("125"));
        assertThat(values.get(0).value.getValue(),is(closeTo(0.10200002,0.4)));
        assertThat(values.get(0).value.getTimeStamp(),equalTo(FEB_2_2021_17_30_Instant.toEpochMilli()));
        assertThat(values.get(1).value.getTimeStamp(),equalTo(recordBaseTime.toEpochMilli()));
        assertThat(values.get(88).value.getValue(),is(closeTo(6.03172715,0.4)));
        assertThat(values.get(88).value.getTimeStamp(),equalTo(FEB_2_2021_17_30_Instant.toEpochMilli()));
        assertThat(values.get(1).key,equalTo("126"));
        assertThat(values.get(384).key,equalTo("356"));
        assertThat(values.get(382).key,equalTo("334"));
        assertThat(values.get(382).value.getTimeStamp(),equalTo(FEB_2_2021_17_30_Instant.toEpochMilli()));

    }

}