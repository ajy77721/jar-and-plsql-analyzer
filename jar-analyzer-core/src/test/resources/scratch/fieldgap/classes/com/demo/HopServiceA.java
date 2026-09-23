package com.demo;

public class HopServiceA {
    private final HopServiceB b;
    public HopServiceA(HopServiceB b) { this.b = b; }

    public void levelOne(String collectionName) {
        b.levelTwo(collectionName);
    }
}
