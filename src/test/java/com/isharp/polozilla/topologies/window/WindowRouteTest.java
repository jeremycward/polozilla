package com.isharp.polozilla.topologies.window;

import com.isharp.polozilla.components.PoloniexSerdes;
import com.isharp.polozilla.topologies.TopologyTestUtil;
import com.isharp.polozilla.vo.Capture;

import com.isharp.polozilla.vo.KeyedPoloCaptureWindow;


import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static com.isharp.polozilla.topologies.TopologyTestUtil.FEB_2_2021_17_30_Instant;
import static com.isharp.polozilla.topologies.TopologyTestUtil.receivingFrom;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class WindowRouteTest {
    private static final String INPUT_TOPIC= RandomStringUtils.randomAlphabetic(15);
    private static final String OUTPUT_TOPIC= RandomStringUtils.randomAlphabetic(15);
    private static final String AGGREGATION_NAME = RandomStringUtils.randomAlphabetic(15);
    private static final Config WINDOW_CONFIG = new Config(INPUT_TOPIC,OUTPUT_TOPIC, Duration.ofMinutes(1),Duration.ofSeconds(1),AGGREGATION_NAME);

    TopologyTestUtil.TimedDataSender priceSender;
    TestOutputTopic<String, KeyedPoloCaptureWindow> windowedMinsTestOutput;
    TopologyTestDriver driver;
    TestInputTopic<String,Capture> inputTopic;
    WindowRoute windowRoute = new WindowRoute();
    Topology underTest = windowRoute.build(WINDOW_CONFIG);

    @Before
    public void setup(){
         driver = TopologyTestUtil.driverFor(underTest,TopologyTestUtil.FRI_8_JAN_2021_17_30_ld);
         driver.getAllStateStores().clear();
         inputTopic=driver.createInputTopic(INPUT_TOPIC, Serdes.String().serializer(),PoloniexSerdes.capture.serializer(), FEB_2_2021_17_30_Instant,Duration.ofMillis(0));
        windowedMinsTestOutput =
        driver.createOutputTopic(OUTPUT_TOPIC,
                Serdes.String().deserializer(),
                                         PoloniexSerdes.keyedPoloCaptureWindow.deserializer());

        driver.createOutputTopic("topic2",Serdes.String().deserializer(),PoloniexSerdes.keyedPoloCaptureWindow.deserializer());
        priceSender = new TopologyTestUtil.TimedDataSender(driver,inputTopic,TopologyTestUtil.FRI_8_JAN_2021_17_30_ld);
    }
    @After
    public void tearDown(){
        driver.close();
    }


    @Test
    public void shouldEmitOneRecWithThreePricesAfterOneInterval()throws Exception{


        priceSender
                .send(TopologyTestUtil.aCapture("eurgbp",100.01d))
                .addTimes(Duration.ofSeconds(1))
                .send(TopologyTestUtil.aCapture("eurgbp",100.02d))
                .addTimes(Duration.ofSeconds(1))
                .send(TopologyTestUtil.aCapture("eurgbp",100.03d))
                .addTimes(Duration.ofMinutes(1))
                .send(TopologyTestUtil.aCapture("eurgbp",999.99d));


        List<TestRecord<String, KeyedPoloCaptureWindow>> results = receivingFrom(windowedMinsTestOutput)
                .expect(1).messages()
                .within(Duration.ofSeconds(10)).results();
        assertThat(results.get(0).getValue().getTicker(),is("eurgbp"));

        assertThat(results.get(0).getValue().getCaptureWindow().getCaptures().size(),is(3));
        assertThat(results.get(0).getValue().getTicker(),is("eurgbp"));
        assertThat(results.get(0).getKey(),is("2021-02-20:1731"));
        assertThat(results.get(0).getRecordTime().toEpochMilli(),is(FEB_2_2021_17_30_Instant.toEpochMilli()+2000));
        assertThat(results.get(0).getValue().getCaptureWindow().getCaptures().get(0).getValue(),is(100.01d));
        assertThat(results.get(0).getValue().getCaptureWindow().getCaptures().get(1).getValue(),is(100.02d));
        assertThat(results.get(0).getValue().getCaptureWindow().getCaptures().get(2).getValue(),is(100.03d));
    }






}

