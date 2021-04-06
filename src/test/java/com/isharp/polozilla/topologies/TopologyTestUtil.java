package com.isharp.polozilla.topologies;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.isharp.polozilla.components.PoloCaptureRecordTimestampExtractor;
import com.isharp.polozilla.components.SampleSpec;
import com.isharp.polozilla.vo.Capture;
import com.isharp.polozilla.vo.KeyedPoloCaptureWindow;
import com.isharp.polozilla.vo.PoloCaptureWindow;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


public class TopologyTestUtil {
    public static final Instant FEB_2_2021_17_30_Instant = Instant.parse("2021-02-20T17:30:43Z").plusMillis(182);

    public static LocalDateTime FRI_8_JAN_2021_17_30_ld = LocalDateTime.ofInstant(FEB_2_2021_17_30_Instant,ZoneId.of("UTC"));
    public static final Instant recordBaseTime = FEB_2_2021_17_30_Instant;
    public static final Duration advance1Milli = Duration.ofMillis(1);


    public static String[] assets = new ImmutableSet.Builder<String>()
            .add(RandomStringUtils.randomAlphabetic(20))
            .add(RandomStringUtils.randomAlphabetic(20))
            .add(RandomStringUtils.randomAlphabetic(20))
            .add(RandomStringUtils.randomAlphabetic(20))
            .add(RandomStringUtils.randomAlphabetic(20))
            .add(RandomStringUtils.randomAlphabetic(20))
            .add(RandomStringUtils.randomAlphabetic(20))
            .build()
            .toArray(new String[0]);



    public static TopologyTestDriver driverFor(Topology top,LocalDateTime startTime){
        Properties mockProps= new Properties();
        mockProps.put("application.id", "polo-tick");
        mockProps.put("bootstrap.servers", "DUMMY_KAFKA_CONFLUENT_CLOUD_9092");
        mockProps.put("default.topic.replication.factor", "1");
        mockProps.put("offset.reset.policy", "latest");
        //mockProps.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
        return new TopologyTestDriver(top,mockProps,startTime.toInstant(ZoneOffset.UTC));
    }

    public static class TimedDataSender<K,V>{

        private LocalDateTime dataTime;
        private final TopologyTestDriver topologyTestDriver;
        private final TestInputTopic<K,V> inputTopic;



        public TimedDataSender(TopologyTestDriver topologyTestDriver, TestInputTopic<K,V> inputTopic,LocalDateTime dataTime) {
            this.topologyTestDriver = topologyTestDriver;
            this.inputTopic = inputTopic;
            this.dataTime = dataTime;

        }


        public TimedDataSender addTimes(Duration d){
            addWallTime(d);
            addDataTime(d);
            return this;
        }

        public TimedDataSender addDataTime(Duration d){
            dataTime = dataTime.plus(d);
            return this;
        }
        public TimedDataSender addWallTime(Duration d){
            topologyTestDriver.advanceWallClockTime(d);
            inputTopic.advanceTime(d);
            return this;
        }
        public TimedDataSender send(Function<TimedDataSender,TestRecord<K,V>> recordSupplier){
            TestRecord rec = recordSupplier.apply(this);
            inputTopic.pipeInput(rec);
           return this;
        }
    }

    public static Function<TimedDataSender,TestRecord<String,Capture>> aCapture(String ticker,Double price){
        return (t)-> {
            Capture capture = new Capture();
            capture.setTimeStamp(toTs(t.dataTime));
            capture.setValue(price);
            return new TestRecord<String, Capture>(ticker,capture,t.dataTime.toInstant(ZoneOffset.UTC));
        };
    }

    public static Function<TimedDataSender,TestRecord<String, KeyedPoloCaptureWindow>> aPoloCaptureWindow(String ticker, Double price, int occurences){
        return (t)-> {

            PoloCaptureWindow window = new PoloCaptureWindow();
            for (int i=0;i<occurences;i++){
                Capture c = new Capture();
                c.setTimeStamp(t.dataTime.toInstant(ZoneOffset.UTC).toEpochMilli());
                c.setValue(price);
                window = window.push(c);
            }
            KeyedPoloCaptureWindow kw = new KeyedPoloCaptureWindow();
            kw.setCaptureWindow(window);
            kw.setTicker(ticker);
            String snapTime = t.dataTime.format(PoloCaptureRecordTimestampExtractor.fmt);

            TestRecord rec = new TestRecord<>(snapTime,kw);
            return rec;


        };
    }







    public static SampleSpecBuilder aSampleSpec(){
        return new SampleSpecBuilder();
    }
    public static CaptureBuilder aCapture(){
        return new CaptureBuilder();

    }
    public static class CaptureBuilder{
        private  LocalDateTime locTime =null;
        private final Double amount = null;
        public Capture andAmount(double amount){
            Capture c = new Capture();
            c.setValue(amount);
            c.setTimeStamp(toTs(locTime));
            return c;
        }
        public CaptureBuilder withTime(LocalDateTime localDateTime){
            locTime = localDateTime;
            return this;
        }
    }
    public static class SampleSpecBuilder{
        private String start;
        private String end;
        private Duration interval;
        public SampleSpecBuilder withStart(String start){
            this.start = start;
            return this;
        }
        public SampleSpecBuilder withEnd(String end){
            this.end = end;
            return this;
        }
        public SampleSpecBuilder withInterval(Duration duration){
            this.interval = duration;
            return this;
        }
        public SampleSpec andAssets(String... assets){
             return new SampleSpec(Lists.newArrayList(assets),LocalTime.parse(start),LocalTime.parse(end),interval);
        }

    }
    public  static LocalDateTime toLd(String isoFormat){
        return LocalDateTime.parse(isoFormat, DateTimeFormatter.ISO_DATE_TIME);
    }
    public static  long toTs(LocalDateTime localDateTime){
        return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public static <K,V>  Receiver<K,V> receivingFrom(TestOutputTopic<K,V> outputTopic){
        return new Receiver<K,V>(outputTopic);

    }
    public static class Receiver<K,V>{
        private final TestOutputTopic<K,V> testOutputTopic;
        private int expectedMessages=0;
        private Duration timeout;

        private List<TestRecord<K,V>> result = Lists.newArrayList();

        public Receiver(TestOutputTopic<K,V> testOutputTopic) {
            this.testOutputTopic = testOutputTopic;
        }
        public Receiver<K,V> expect(int messageCount){
            this.expectedMessages = messageCount;
            return this;
        }
        public Receiver<K,V> messages(){
            return this;
        }
        public Receiver<K,V> within(Duration d){
            this.timeout = d;
            return this;

        }
        static ExecutorService executor = Executors.newFixedThreadPool(2) ;

        public List<TestRecord<K, V>> results() {
            CountDownLatch cdl = new CountDownLatch(expectedMessages);
            long start = System.currentTimeMillis();
            Runnable poller = new Runnable() {
                @Override
                public void run() {
                    while(result.size()<expectedMessages){
                        if (!testOutputTopic.isEmpty()){
                            result.add(testOutputTopic.readRecord());
                            cdl.countDown();
                        }
                    }
                }
            };
            try {

                executor.submit(poller);
                cdl.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
                if (result.size()>= expectedMessages){
                    return result;

                }else{
                    throw new RuntimeException("timed out with record count " + result.size());
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }




}
