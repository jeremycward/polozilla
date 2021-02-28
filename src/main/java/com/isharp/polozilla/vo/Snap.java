package com.isharp.polozilla.vo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class Snap {

    private String snapTime;
    private  Map<String, List<Capture>> capture= Maps.newHashMap();


    public String getSnapTime() {
        return snapTime;
    }

    public void setSnapTime(String snapTime) {
        this.snapTime = snapTime;
    }
    public Snap add(String asset,PoloCaptureWindow poloCaptureWindow){
        List<Capture> captures = capture.computeIfAbsent(asset,(key)-> Lists.newArrayList());
        captures.addAll(poloCaptureWindow.getCaptures());
        return this;
    }

    public Map<String, List<Capture>> getCapture() {
        return capture;
    }

    public void setCapture(Map<String, List<Capture>> capture) {
        this.capture = capture;
    }
}
