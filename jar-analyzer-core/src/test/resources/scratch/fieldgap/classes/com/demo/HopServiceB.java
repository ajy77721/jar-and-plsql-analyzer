package com.demo;

/** Hop 2: pure pass-through. */
public class HopServiceB {
    private final HopRepoLeaf leaf;
    public HopServiceB(HopRepoLeaf leaf) { this.leaf = leaf; }

    public void levelTwo(String collectionName) {
        leaf.execute(collectionName);
    }
}
