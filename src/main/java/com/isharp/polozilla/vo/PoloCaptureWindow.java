package com.isharp.polozilla.vo;



import java.util.Stack;

public class PoloCaptureWindow {
    private final Stack<Capture> captures = new Stack<>();

    public PoloCaptureWindow() {
    }

    public Stack<Capture> getCaptures() {
        return captures;
    }
    public  PoloCaptureWindow push(Capture c){
        captures.push(c);
        return this;
    }


}
