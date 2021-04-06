package com.isharp.ploniexfeed.connect;
import com.isharp.polozilla.vo.Snap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Windowed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerServiceImpl implements KafkaConsumerService{
    final Logger logger = LoggerFactory.getLogger(KafkaConsumerServiceImpl.class);
   @KafkaListener(topics = "${kafka.tickers.1.min.snaps}")
    public void receive(@Payload ConsumerRecord<Windowed,Snap> data) {
        Snap snap =  data.value();

        logger.info("received snap with {} assets,key={}", snap.getCapture().size(),data.key().key());
    }

}