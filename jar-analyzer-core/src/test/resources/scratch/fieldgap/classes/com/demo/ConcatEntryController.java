package com.demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-003: the entry point assigns the literals that feed the concat one level down. */
@RestController
public class ConcatEntryController {
    private final ConcatService service;
    public ConcatEntryController(ConcatService service) { this.service = service; }

    @GetMapping("/orders-concat")
    public String handle() {
        String east = "east";
        service.buildQuery(east);
        return "ok";
    }
}
