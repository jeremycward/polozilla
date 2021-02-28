package com.isharp.polozilla.vo;

public class KeyedPoloCaptureWindow {
     String ticker;
     PoloCaptureWindow captureWindow;

    public KeyedPoloCaptureWindow(String ticker, PoloCaptureWindow captureWindow) {
        this.ticker = ticker;
        this.captureWindow = captureWindow;
    }

    public KeyedPoloCaptureWindow() {
    }

    public String getTicker() {
        return ticker;
    }

    public void setTicker(String ticker) {
        this.ticker = ticker;
    }

    public PoloCaptureWindow getCaptureWindow() {
        return captureWindow;
    }

    public void setCaptureWindow(PoloCaptureWindow captureWindow) {
        this.captureWindow = captureWindow;
    }
}
