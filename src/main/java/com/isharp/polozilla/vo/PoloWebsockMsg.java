package com.isharp.polozilla.vo;


import com.google.common.collect.Lists;
import com.google.gson.stream.JsonReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.List;

public class PoloWebsockMsg {
    private Integer msgType;
    List<PoloTick> tickList;

    public PoloWebsockMsg(Integer msgType, List<PoloTick> tickList) {
        this.msgType = msgType;
        this.tickList = tickList;
    }

    public List<PoloTick> getTickList() {
        return tickList;
    }

    public Integer getMsgType() {
        return msgType;
    }

    public void setMsgType(Integer msgType) {
        this.msgType = msgType;
    }

    public static PoloWebsockMsg from(String inputRecord){
        try {
            JsonReader jsonReader = new JsonReader(new StringReader(inputRecord));
            jsonReader.beginArray();
            int i = jsonReader.nextInt();
            List<PoloTick> ticks = Lists.newArrayList();
            if (i==1002 &&  inputRecord.length()>50){
                jsonReader.nextNull();
                while (jsonReader.hasNext()) {
                    jsonReader.beginArray();
                    ticks.add(PoloTick.fromReader(jsonReader));
                    jsonReader.endArray();
                }
            }
            return new PoloWebsockMsg(i,ticks);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
