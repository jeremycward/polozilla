package com.isharp.polozilla.topologies.snap;

import com.isharp.polozilla.components.PoloniexSerdes;
import com.isharp.polozilla.topologies.TopologyTestUtil;
import com.isharp.polozilla.vo.KeyedPoloCaptureWindow;
import com.isharp.polozilla.vo.Snap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static com.isharp.polozilla.topologies.TopologyTestUtil.FEB_2_2021_17_30_Instant;
import static com.isharp.polozilla.topologies.TopologyTestUtil.receivingFrom;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class SnapRouteTest {
    private static final String INPUT_TOPIC= RandomStringUtils.randomAlphabetic(15);
    private static final String OUTPUT_TOPIC= RandomStringUtils.randomAlphabetic(15);
    private static final com.isharp.polozilla.topologies.snap.Config WINDOW_CONFIG =
            new com.isharp.polozilla.topologies.snap.Config(INPUT_TOPIC,OUTPUT_TOPIC, Duration.ofMinutes(1),Duration.ofSeconds(1));

    TopologyTestUtil.TimedDataSender priceSender;
    TestOutputTopic<Windowed<String>, Snap> windowedMinsTestOutput;
    TopologyTestDriver driver;
    TestInputTopic<String, KeyedPoloCaptureWindow> inputTopic;
    SnapRoute snapRoute = new SnapRoute();
    Topology underTest = snapRoute.build(WINDOW_CONFIG);

    @Before
    public void setup(){
         driver = TopologyTestUtil.driverFor(underTest,TopologyTestUtil.FRI_8_JAN_2021_17_30_ld);
         driver.getAllStateStores().clear();
         inputTopic=driver.createInputTopic(INPUT_TOPIC, Serdes.String().serializer(),PoloniexSerdes.keyedPoloCaptureWindow.serializer() , FEB_2_2021_17_30_Instant,Duration.ofMillis(0));
        windowedMinsTestOutput =
        driver.createOutputTopic(OUTPUT_TOPIC,
                WindowedSerdes.sessionWindowedSerdeFrom(String.class).deserializer(),
                                         PoloniexSerdes.snap.deserializer());
        priceSender = new TopologyTestUtil.TimedDataSender(driver,inputTopic,TopologyTestUtil.FRI_8_JAN_2021_17_30_ld);
    }
    @After
    public void tearDown(){
        driver.close();
    }


    @Test
    public void shouldEmitOneRecWithThreePricesAfterOneInterval()throws Exception{

        priceSender
                .send(TopologyTestUtil.aPoloCaptureWindow("eurusd", 0.001d,10))
                .addTimes(Duration.ofMillis(1))
                .send(TopologyTestUtil.aPoloCaptureWindow("eurusd", 0.002d,9))

                .addTimes(Duration.ofMillis(1))
                .send(TopologyTestUtil.aPoloCaptureWindow("gbpusd", 0.001d,8))
                .addTimes(Duration.ofMillis(1))
                .send(TopologyTestUtil.aPoloCaptureWindow("gbpusd", 1.002d,7))

                .addTimes(Duration.ofMinutes(5))
                .send(TopologyTestUtil.aPoloCaptureWindow("gbpusd", 2.002d,17));







        List<TestRecord<Windowed<String>, Snap>> results = receivingFrom(windowedMinsTestOutput)
                .expect(1).messages()
                .within(Duration.ofSeconds(10)).results();

        assertThat(results.get(0).getValue().getCapture().get("eurusd").size(), is(19));
        assertThat(results.get(0).getValue().getCapture().get("eurusd").get(18).getValue(),is(0.002d));
        assertThat(results.get(0).getValue().getCapture().get("gbpusd").size(), is(15));
        assertThat(results.get(0).getValue().getCapture().get("gbpusd").get(0).getValue(),is(0.001d));
        assertThat(results.get(0).getValue().getCapture().get("gbpusd").get(14).getValue(),is(1.002d));


        //assertThat(results.get(0).getKey().key(),is("2021-02-20:1730"));
    }






}

