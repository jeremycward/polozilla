package com.isharp.polozilla.topologies.window;

import com.google.common.collect.ArrayListMultimap;
import com.isharp.polozilla.components.PoloniexSerdes;
import com.isharp.polozilla.topologies.TopologyTestUtil;
import com.isharp.polozilla.vo.Capture;
import com.isharp.polozilla.vo.PoloCaptureWindow;
import com.isharp.polozilla.vo.PoloSnapKey;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;


public class RouteTest {
    private static final String INPUT_TOPIC= RandomStringUtils.randomAlphabetic(15);
    private static final String OUTPUT_TOPIC= RandomStringUtils.randomAlphabetic(15);


    TestInputTopic<String,Capture> inputTopic;
    TestOutputTopic<PoloSnapKey, PoloCaptureWindow> windowedSecsTestOutput;
    Config cfg = testConfigWindow(50);

    @Before
    public void setup(){

        Route windowRoute = new Route();
        Topology underTest = windowRoute.build(cfg);
        TopologyTestDriver driver = TopologyTestUtil.driverFor(underTest);
        inputTopic = driver.createInputTopic(INPUT_TOPIC, Serdes.String().serializer(),PoloniexSerdes.capture.serializer());
        windowedSecsTestOutput =
                driver.createOutputTopic(OUTPUT_TOPIC,
                                         PoloniexSerdes.snapKey.deserializer(),
                                         PoloniexSerdes.captureWindow.deserializer());
    }
    private Config testConfigWindow(int millis){
        return new Config(INPUT_TOPIC,OUTPUT_TOPIC, millis);
    }


    @Test
    public void shouldSerializeAndFlattenOneRec()throws Exception{


        new TopologyTestUtil.PriceSender(inputTopic)
                .sendAPrice("eurjpy",100)
                .advanceMillis(1)
                .sendAPrice("usdjpy",1000)
                .advanceMillis(1)
                .sendAPrice("eurjpy",101)
                .advanceMillis(1)
                .sendAPrice("usdjpy",1001)
                .advanceMillis(1)
                .sendAPrice("eurjpy",102)
                .advanceMillis(1)
                .sendAPrice("usdjpy",1002)
                .advanceMillis(cfg.getTimeWindoMillis())
                .sendAPrice("audusd",0)
                .advanceMillis(cfg.getTimeWindoMillis())
                .sendAPrice("eursek",0);



        List<KeyValue<PoloSnapKey, PoloCaptureWindow>> values = windowedSecsTestOutput.readKeyValuesToList();
        assertThat(values.size(),is(3));

        List<KeyValue<PoloSnapKey,PoloCaptureWindow>> audusdValues =
                values.stream().filter(it->it.key.getTicker().equals("audusd")).collect(Collectors.toList());

        assertThat(audusdValues.size(),is(1));
        assertThat(audusdValues.get(0).value.getCaptures().size(),is(1));


        List<KeyValue<PoloSnapKey,PoloCaptureWindow>> eurJpyValues =
                values.stream().filter(it->it.key.getTicker().equals("eurjpy")).collect(Collectors.toList());
        assertThat(eurJpyValues.size(),is(1));
        assertThat(eurJpyValues.get(0).value.getCaptures().size(),is(3));


        List<KeyValue<PoloSnapKey,PoloCaptureWindow>> usdJpyValues =
                values.stream().filter(it->it.key.getTicker().equals("usdjpy")).collect(Collectors.toList());

        assertThat(usdJpyValues.size(),is(1));
        KeyValue<PoloSnapKey,PoloCaptureWindow> usdJpyCaptures = usdJpyValues.get(0);
        assertThat(usdJpyCaptures.key.getTicker(),is("usdjpy"));
        assertThat(usdJpyCaptures.value.getCaptures().size(),is(3));

        assertThat(usdJpyCaptures.value.getCaptures().get(0).getValue(),is(1000.0));
        assertThat(usdJpyCaptures.value.getCaptures().get(1).getValue(),is(1001.0));
        assertThat(usdJpyCaptures.value.getCaptures().get(2).getValue(),is(1002.0));

        long firstWindoEnd = eurJpyValues.get(0).key.getEndTime();
        long lastWindowEnd = audusdValues.get(0).key.getEndTime();
        assertThat(lastWindowEnd - firstWindoEnd, is (cfg.getTimeWindoMillis()) );







    }






}

