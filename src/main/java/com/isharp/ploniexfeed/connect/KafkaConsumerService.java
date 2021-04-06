package com.isharp.ploniexfeed.connect;

import com.isharp.polozilla.vo.Snap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.kstream.Windowed;

public interface KafkaConsumerService {
    void receive(ConsumerRecord<Windowed, Snap> data);
}
