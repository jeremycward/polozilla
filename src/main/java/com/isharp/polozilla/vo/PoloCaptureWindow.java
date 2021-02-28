package com.isharp.polozilla.vo;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

public class PoloCaptureWindow {
    private final List<Capture> captures;
    public PoloCaptureWindow() {
        this(Lists.newArrayList());
    }

    public PoloCaptureWindow(List<Capture> captures) {
        this.captures = captures;
    }
    public  PoloCaptureWindow push(Capture c){
        return new PoloCaptureWindow(new ImmutableList.Builder<Capture>().addAll(this.captures).add(c).build());
    }
    public List<Capture> getCaptures() {
        return captures;
    }


}
