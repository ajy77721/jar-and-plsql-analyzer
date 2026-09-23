package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/** CALL-003 negative: BOTH operands are genuinely runtime values — must stay unresolved. */
@RestController
public class NegativeConcatController {
    private final com.demo.ConcatRepoLeaf leaf;
    public NegativeConcatController(com.demo.ConcatRepoLeaf leaf) { this.leaf = leaf; }

    @GetMapping("/orders-negative-concat")
    public String listOrdersNegativeConcat(@PathVariable String region) {
        String zone = System.getProperty("demo.zone", "zone");
        String collection = region + zone;
        leaf.executeQuery(collection);
        return "ok";
    }
}
