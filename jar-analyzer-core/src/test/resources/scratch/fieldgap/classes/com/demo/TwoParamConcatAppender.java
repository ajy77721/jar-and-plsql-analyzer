package com.demo;

/** Hop 2: the concat — NEITHER operand is a same-method literal here. */
public class TwoParamConcatAppender {
    private final TwoParamConcatRepoLeaf leaf;
    public TwoParamConcatAppender(TwoParamConcatRepoLeaf leaf) { this.leaf = leaf; }

    public void append(String prefix, String region) {
        String collection = prefix + region;
        leaf.execute(collection);
    }
}
