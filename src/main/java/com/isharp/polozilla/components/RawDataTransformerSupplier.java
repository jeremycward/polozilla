package com.isharp.polozilla.components;


import com.isharp.polozilla.vo.PoloWebsockMsg;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;


import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;


import java.util.Collections;
import java.util.Set;

public class RawDataTransformerSupplier implements TransformerSupplier<String, String, KeyValue<Long, PoloWebsockMsg>>{

    private Transformer<String, String, KeyValue<Long, PoloWebsockMsg>> rawBytesTransformer = new Transformer<String, String, KeyValue<Long,PoloWebsockMsg>>() {
        private ProcessorContext context = null;
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<Long,PoloWebsockMsg> transform(String key, String value) {
            String trimmed = PoloniexSerdes.trimToJsonStart(new String(value));
                PoloWebsockMsg msg = PoloWebsockMsg.from(trimmed.trim());
                return KeyValue.pair(context.timestamp(), msg);
        }

        @Override
        public void close() {

        }
    };


    @Override
    public Transformer<String, String, KeyValue<Long, PoloWebsockMsg>> get() {
        return rawBytesTransformer;
    }

    @Override
    public Set<StoreBuilder<?>> stores() {
        return Collections.emptySet();
    }
};




