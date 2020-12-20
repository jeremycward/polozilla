package com.isharp.polozilla.components;

import com.google.gson.GsonBuilder;

import com.isharp.polozilla.vo.Capture;
import com.isharp.polozilla.vo.PoloCaptureWindow;
import com.isharp.polozilla.vo.PoloSnapKey;
import com.isharp.polozilla.vo.PoloWebsockMsg;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class PoloniexSerdes {
    static Deserializer<PoloWebsockMsg> rabbitMqDeser = new Deserializer<PoloWebsockMsg>() {
        @Override
        public PoloWebsockMsg deserialize(String topic, byte[] data) {
            return PoloWebsockMsg.from(trimToJsonStart(new String(data)));
        }
    };
    private static final GsonBuilder gsonBuilder = new GsonBuilder();

    public static String trimToJsonStart(String untrimmed){
        int startAt = untrimmed.indexOf("[");
        String trimmed = untrimmed.substring(startAt);
        return trimmed;
    }




    static final class PoloWebSockSerde extends Serdes.WrapperSerde<PoloWebsockMsg>{
        public PoloWebSockSerde() {
            super(new Serializer<PoloWebsockMsg>() {
                @Override
                public byte[] serialize(String topic, PoloWebsockMsg data) {
                    throw new UnsupportedOperationException();
                }
            }, rabbitMqDeser);
        }
    }





    static final class GsonSerialiser<T> implements  Serializer<T>{
        @Override
        public byte[] serialize(String s, T t) {
            return gsonBuilder.create().toJson(t).getBytes();
        }
    }
    static final class GsonDeSerialiser<T> implements  Deserializer<T>{
        private final Class cl ;

        public GsonDeSerialiser(Class cl) {
            this.cl = cl;
        }

        @Override
        public T deserialize(String s, byte[] bytes) {
            return (T) gsonBuilder.create().fromJson(new String(bytes),cl);
        }
    }

    static final class GsonSerde <T> extends Serdes.WrapperSerde<T>{
        public GsonSerde(Class<T> clz) {
            super(new GsonSerialiser<>(), new GsonDeSerialiser<>(clz));
        }
    }


    public static final Serdes.WrapperSerde<PoloCaptureWindow> captureWindow = new GsonSerde<>(PoloCaptureWindow.class);
    public static final Serdes.WrapperSerde<Capture> capture = new GsonSerde<>(Capture.class);
    public static final Serdes.WrapperSerde<PoloSnapKey> snapKey = new GsonSerde<>(PoloSnapKey.class);

}
