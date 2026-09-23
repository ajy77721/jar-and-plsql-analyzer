package demo;

import org.springframework.web.client.RestTemplate;

/** NEITHER operand of the concat is a same-method literal — both resolve from the caller. */
public class OrderClient {
    private final RestTemplate rest;
    public OrderClient(RestTemplate rest) { this.rest = rest; }

    public void fetch(String basePath, String region) {
        String url = basePath + region;
        rest.getForObject(url, String.class);
    }
}
