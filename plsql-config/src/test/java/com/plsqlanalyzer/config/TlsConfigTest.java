package com.plsqlanalyzer.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the TLS/connection-config contract on ConnectionConfig:
 * TLS is opt-in (NONE == no driver properties), and a relative PEM trust path is resolved
 * against the supplied config directory and converted to a sibling JKS twin for drivers
 * that cannot consume PEM directly.
 */
class TlsConfigTest {

    /** Minimal self-signed X.509 cert (CN=test.local) used purely as a parseable PEM fixture. */
    private static final String SELF_SIGNED_PEM = """
            -----BEGIN CERTIFICATE-----
            MIIDCzCCAfOgAwIBAgIUew2iSBxoE3CBeKFiRKFcjZfGNDkwDQYJKoZIhvcNAQEL
            BQAwFTETMBEGA1UEAwwKdGVzdC5sb2NhbDAeFw0yNjA5MjMxOTIzMDVaFw0zNjA5
            MjAxOTIzMDVaMBUxEzARBgNVBAMMCnRlc3QubG9jYWwwggEiMA0GCSqGSIb3DQEB
            AQUAA4IBDwAwggEKAoIBAQDSfHvtbW7y1Cl815QttlUwIqB1Dwqm0lYMlSR3MISM
            yokl1PRIeWC5pEK5RsplQZCCz1lkx4LkM04YujOhSfhGJJ5PfJ70f8VK+1zgHy5y
            QKgSwjbxkUQIuqA8WRDQbUNAvdUlwkCg6VbMSD4RfvyMZyEq2Ajw8WsyP14NLl+Y
            5bGLUUHOpC1P6PvdF9vmizG/RxgPlTcE0kUWicV+WBNWBcIX0v4JghN7eaXQMIUt
            UfJno4NjCb2kXJkkl9IgpTt77Bfd8PS8973NUvvQx0i+D/66ZgjPBtSuXR9aK4AG
            3ql437g54Vh9g/AxqPfML64tHB+ZZKD198wNi+lhJignAgMBAAGjUzBRMB0GA1Ud
            DgQWBBTHahGd8isAABH11ZxNFJEOs0iEvDAfBgNVHSMEGDAWgBTHahGd8isAABH1
            1ZxNFJEOs0iEvDAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQB7
            5RT2xdNgyXsoWQoYTISOEBRBoqtC6jmKtouNaKTk5rxi6emALJZac4aZjZQLDndF
            qEFev/pqM4Mw8jaQ5hcBQ9IhAbvnbtRnkZ122b8JZntTZHY5JJ0hXF6ItG7h0+d3
            Ufk7rxx4Iai6rY7tzThcwjWrttRXbWZ8i0SOpnLBy3YMEWz/g6mo9yUias3y4yop
            x2z3KrDnZPGz6XoalRMD9ImqjIZnQpskx7Nz5VewpgrQ1pUW3rR78fhP4hh8qIcp
            RDoUsLVgosWCPaOPXbxnPseMRca2WPxsWEc9anbTjABw3PJuAQtwdqn6ar67pT89
            1+bX3MDLCZ5F+RcAI3qd
            -----END CERTIFICATE-----
            """;

    @Test
    void sslModeNone_yieldsEmptyProperties_andTlsDisabled() {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("DB2");
        assertFalse(c.isTlsEnabled(), "null sslMode must mean TLS disabled");

        Properties props = c.buildSslProperties(null);
        assertTrue(props.isEmpty(), "TLS disabled must produce an empty property set");

        c.setSslMode("NONE");
        assertFalse(c.isTlsEnabled(), "explicit NONE must mean TLS disabled");
        assertTrue(c.buildSslProperties(null).isEmpty());
    }

    @Test
    void verifyCa_relativeTrustCert_isResolvedAndJksTwinCreated(@TempDir Path configDir) throws Exception {
        Path certsDir = Files.createDirectories(configDir.resolve("certs/MY_CONN"));
        Path pem = certsDir.resolve("trust.pem");
        Files.writeString(pem, SELF_SIGNED_PEM);

        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("DB2");
        c.setSslMode("VERIFY_CA");
        c.setTrustCertPath("certs/MY_CONN/trust.pem");
        assertTrue(c.isTlsEnabled());

        Properties props = c.buildSslProperties(configDir);

        assertEquals("true", props.getProperty("sslConnection"));
        assertEquals("JKS", props.getProperty("sslTrustStoreType"));

        Path jks = certsDir.resolve("trust.jks");
        assertTrue(Files.exists(jks), "PEM trust cert must be converted to a sibling JKS twin");
        assertEquals(jks.toAbsolutePath().normalize().toString(),
                props.getProperty("sslTrustStoreLocation"),
                "relative trust_cert_path must be resolved against the config dir");
        assertNotNull(props.getProperty("sslTrustStorePassword"));
        assertFalse(props.getProperty("sslTrustStorePassword").isBlank());
    }

    @Test
    void tlsDisabled_buildJdbcUrlIgnoresTlsOnDialects(@TempDir Path configDir) {
        ConnectionConfig c = new ConnectionConfig();
        c.setDbType("POSTGRES");
        c.setHostname("db.example.com");
        c.setPort(5432);
        c.setServiceName("appdb");
        // No sslMode set → the legacy plaintext URL, even with a config dir supplied.
        assertEquals("jdbc:postgresql://db.example.com:5432/appdb", c.getJdbcUrl(configDir));
        assertNull(c.buildSslProperties(configDir).getProperty("sslmode"));
    }
}
