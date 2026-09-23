package demo;

import org.springframework.web.client.RestTemplate;

/** The concat is built one level below the caller that supplies the literal region. */
public class ReportClient {
    private final RestTemplate rest;
    public ReportClient(RestTemplate rest) { this.rest = rest; }

    public void fetch(String region) {
        String url = "/api/orders/" + region;
        rest.getForObject(url, String.class);
    }
}
