package com.demo;

public class UnannotatedClassServiceMid {
    private final UnannotatedClassRepoLeaf leaf;
    public UnannotatedClassServiceMid(UnannotatedClassRepoLeaf leaf) { this.leaf = leaf; }

    public void fetch(Class<?> entityClass) {
        leaf.execute(entityClass);
    }
}
