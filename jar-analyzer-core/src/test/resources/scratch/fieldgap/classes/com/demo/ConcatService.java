package com.demo;

/** The concat happens AT THIS CALL SITE: single-assignment local, then the leaf call. */
public class ConcatService {
    private final ConcatRepoLeaf leaf;
    public ConcatService(ConcatRepoLeaf leaf) { this.leaf = leaf; }

    public void buildQuery(String east) {
        String collection = "orders_" + east;
        leaf.executeQuery(collection);
    }
}
