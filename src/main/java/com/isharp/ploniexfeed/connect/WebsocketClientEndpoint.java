package com.isharp.ploniexfeed.connect;


import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.isharp.polozilla.components.RawRecordTImestampExtractor;
import com.isharp.polozilla.topologies.flatten.Config;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.websocket.*;
import java.net.URI;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.Properties;

/**
 * ChatServer Client
 *
 * @author Jiji_Sasidharan
 */

@ClientEndpoint
@Component
@DependsOn("kafkaTemplate")
public class WebsocketClientEndpoint {
    final Logger logger = LoggerFactory.getLogger(WebsocketClientEndpoint.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value(value = "${poloniex.endpoint}")
    private String poloniexEndpoint;

    @Value(value="${kafka.tickers.input}")
    private String tickersInTopic;

    @Autowired
    @Qualifier("flatten_topology")
    Topology flatten;

    @Autowired
    @Qualifier("tickers_window_secs")
    Topology window_secs;

    @Value(value = "${kafka.bootstrapAddress}")
    private String bootstrapAddress;



    @Autowired
    @Qualifier("tickers_window_mins")
    Topology window_mins;

    @Autowired
    @Qualifier("tickers_mins_snaps")
    Topology snaps;

    Session userSession = null;


    public WebsocketClientEndpoint() {
    }
    @PostConstruct
    public void startSubscription(){
        Properties kafkaSystemConfig = new Properties();
        kafkaSystemConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,   bootstrapAddress);
        private String bootstrapAddress;
);
        kafkaSystemConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        kafkaSystemConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        startAStream(kafkaSystemConfig,"POL.FLATTEN",flatten);
        startAStream(kafkaSystemConfig,"POL.WINDOWED.SECS",window_secs);
        startAStream(kafkaSystemConfig,"POL.WINDOWED.MINS",window_mins);
        startAStream(kafkaSystemConfig,"POL.1.MIN.SNAPS_STREAM",snaps);


        try {
            WebSocketContainer container = ContainerProvider.getWebSocketContainer();
            container.connectToServer(this, new URI("wss://api2.poloniex.com"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    private static final void  startAStream(Properties source,String appId,Topology topology){
        Properties clonse = (Properties)source.clone();
        clonse.put(StreamsConfig.APPLICATION_ID_CONFIG,appId);
        final KafkaStreams windowStream = new KafkaStreams(topology,clonse);
        windowStream.start();
    }

    /**
     * Callback hook for Connection open events.
     *
     * @param userSession the userSession which is opened.
     */
    @OnOpen
    public void onOpen(Session userSession) {
        logger.info("opening websocket and subscribing");
        this.userSession = userSession;
        String subMsg = PoloniexWssSubscription.TICKER.toString();
        sendMessage(subMsg);
    }

    /**
     * Callback hook for Connection close events.
     *
     * @param userSession the userSession which is getting closed.
     * @param reason the reason for connection close
     */
    @OnClose
    public void onClose(Session userSession, CloseReason reason) {

        this.userSession = null;
    }

    /**
     * Callback hook for Message Events. This method will be invoked when a client send a message.
     *
     * @param message The text message
     */
    @OnMessage
    public void onMessage(String message) {

            if(message.contains("[1010]")||message.equals("[1002,1]")) {
                logger.info("Subscription admin msg {}", message);
            }else{
                LocalDateTime n = LocalDateTime.now();
                String formatted = RawRecordTImestampExtractor.formatDatePrefix(n);

                try {
                    this.kafkaTemplate.send(tickersInTopic, formatted,message);
                } catch (Exception e) {
                    logger.error("Error on sending", e);
                }
            }

    }


    /**
     * Send a message.
     *
     * @param message
     */
    public void sendMessage(String message) {
        this.userSession.getAsyncRemote().sendText(message);
    }

}