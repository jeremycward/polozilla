package com.isharp.polozilla.vo;

import java.util.Stack;

public class Snap {
    private Stack<Capture> captures = new Stack<>();

    public Stack<Capture> getCaptures() {
        return captures;
    }

    public void setCaptures(Stack<Capture> captures) {
        this.captures = captures;
    }
    public Snap addCapture(Capture c){
        captures.push(c);
        return this;
    }
}
