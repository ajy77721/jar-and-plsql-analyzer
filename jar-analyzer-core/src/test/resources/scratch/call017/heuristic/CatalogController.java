package demo;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** CALL-017 heuristic multi-hop: NO @Document ground truth — naming convention must supply it. */
@RestController
public class CatalogController {
    private final CatalogService service;
    public CatalogController(CatalogService service) { this.service = service; }

    @GetMapping("/catalogs")
    public void catalogs() {
        service.levelOne(CatalogEntity.class);
    }
}
