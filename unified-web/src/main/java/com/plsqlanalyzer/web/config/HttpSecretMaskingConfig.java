package com.plsqlanalyzer.web.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.plsqlanalyzer.config.ConnectionConfig;
import com.plsqlanalyzer.config.DbUserConfig;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Keeps database passwords out of every HTTP response.
 *
 * <p>{@code GET /api/plsql/config} serialised {@link ConnectionConfig} directly, so it returned the
 * plaintext production database passwords to any unauthenticated caller — and
 * {@code ApiLoggingFilter} then wrote that same body into the application log file.
 *
 * <p>The fix is applied as a <b>mixin on the HTTP ObjectMapper only</b>, deliberately, rather than
 * as an annotation on {@code ConnectionConfig} itself. {@code ConfigLoader.saveToFile} persists the
 * very same object graph with its own YAML {@code ObjectMapper}; annotating the model would have
 * hidden the field from that writer too, so the next config save would have written empty
 * passwords over {@code plsql-config.yaml} and destroyed every stored credential. Two mappers, two
 * different requirements — so the rule belongs on the mapper that serves HTTP, not on the model.
 *
 * <p>Because it is registered on the mapper, it covers <i>every</i> endpoint that returns a
 * {@code ConnectionConfig}, including ones added later. An endpoint-by-endpoint fix would only
 * cover the ones known today.
 *
 * @see ConnectionConfigMixin
 */
@Configuration
public class HttpSecretMaskingConfig {

    /**
     * Hides the password on serialisation while leaving {@code setPassword} bound, so a client can
     * still submit a new one.
     *
     * <p>Note the consequence for the connection editor: it can no longer pre-fill the password
     * input, so an unchanged form now submits a blank value. The write path in
     * {@code ConfigController} therefore treats blank as "leave the stored password alone" — an
     * empty password is not a value anyone can legitimately save.
     *
     * <p><b>Real bug found live:</b> {@code @JsonIgnore} on just the getter does NOT do what the
     * comment above claims — Jackson treats {@code @JsonIgnore} on either accessor as ignoring the
     * whole logical property in BOTH directions unless another accessor for the same property is
     * explicitly re-enabled. With no such override, every incoming request body carrying a real
     * password (e.g. "Test Connection" on a brand-new, not-yet-saved connection, or the standalone
     * test-any-connection endpoint) silently deserialised to a null password — "leaving setPassword
     * bound" was never actually true. The explicit {@code @JsonProperty} on the setter below is
     * what actually re-enables deserialisation; it does not affect serialisation, so passwords are
     * still never written back out to any HTTP response.
     */
    abstract static class ConnectionConfigMixin {
        @JsonIgnore
        abstract String getPassword();
        @JsonProperty("password")
        abstract void setPassword(String password);
    }

    /**
     * {@link DbUserConfig} carries a password too, and it is what the per-connection user list
     * actually serialises — covering only {@code ConnectionConfig} left every one of them exposed.
     * Verified against a running instance rather than assumed.
     *
     * <p>Same one-getter-only mistake as {@link ConnectionConfigMixin} above, and the same fix.
     */
    abstract static class DbUserConfigMixin {
        @JsonIgnore
        abstract String getPassword();
        @JsonProperty("password")
        abstract void setPassword(String password);
    }

    @Bean
    Jackson2ObjectMapperBuilderCustomizer maskSecretsInHttpResponses() {
        return builder -> builder
                .mixIn(ConnectionConfig.class, ConnectionConfigMixin.class)
                .mixIn(DbUserConfig.class, DbUserConfigMixin.class);
    }
}
