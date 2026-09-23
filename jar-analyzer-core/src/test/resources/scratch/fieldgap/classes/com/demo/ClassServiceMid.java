package com.demo;

/** Hop 1: forwards its OWN parameter unchanged. */
public class ClassServiceMid {
    private final ClassRepoLeaf leaf;
    public ClassServiceMid(ClassRepoLeaf leaf) { this.leaf = leaf; }

    public void fetch(Class<?> entityClass) {
        leaf.execute(entityClass);
    }
}
