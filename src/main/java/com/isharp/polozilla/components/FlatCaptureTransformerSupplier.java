package com.isharp.polozilla.components;

import com.isharp.polozilla.vo.Capture;
import com.isharp.polozilla.vo.PoloWebsockMsg;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ConnectedStoreProvider;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class FlatCaptureTransformerSupplier implements TransformerSupplier<Long, PoloWebsockMsg, Iterable<KeyValue<String, Capture>>>, ConnectedStoreProvider {

    private final Transformer<Long, PoloWebsockMsg,Iterable<KeyValue<String,Capture>>> transformer = new Transformer<Long, PoloWebsockMsg, Iterable<KeyValue<String, Capture>>>(){
        @Override
        public void init(ProcessorContext context) {

        }

        @Override
        public Iterable<KeyValue<String, Capture>> transform(Long key, PoloWebsockMsg value) {
            List<KeyValue<String, Capture>> results = value.getTickList().stream().map(it -> {
                Capture cap = new Capture();
                cap.setTimeStamp(key);
                cap.setValue(Double.parseDouble(it.getLastTradePrie()));
                String ccyPair = Long.toString(it.getCcyPairId());
                return KeyValue.pair(ccyPair, cap);

            }).collect(Collectors.toList());
            return results;
        }

        @Override
        public void close() {

        }
    };

    @Override
    public Set<StoreBuilder<?>> stores() {
        return null;
    }

    @Override
    public Transformer<Long, PoloWebsockMsg, Iterable<KeyValue<String, Capture>>> get() {
        return transformer;
    }
}
