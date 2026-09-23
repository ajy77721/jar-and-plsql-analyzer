package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-017 confirmed multi-hop: the Class<?> threads through 2 wrappers before the ES call. */
@RestController
public class CatalogController {
    private final CatalogService service;
    public CatalogController(CatalogService service) { this.service = service; }

    @GetMapping("/catalog")
    public void catalog() {
        service.levelOne(Catalog.class);
    }
}
