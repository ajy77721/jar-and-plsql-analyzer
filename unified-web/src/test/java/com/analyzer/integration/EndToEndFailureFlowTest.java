package com.analyzer.integration;

import com.analyzer.queue.ApiExceptionAdvice;
import com.analyzer.storage.ArtifactKey;
import com.analyzer.storage.ArtifactStore;
import com.analyzer.storage.sso.NotAuthenticatedException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jaranalyzer.service.JarDataPaths;
import com.jaranalyzer.service.PersistenceService;
import com.jaranalyzer.service.StorageListingException;
import com.plsqlanalyzer.config.DbUserConfig;
import com.plsqlanalyzer.config.PlsqlConfig;
import com.plsqlanalyzer.web.controller.DatabaseController;
import com.plsqlanalyzer.web.page.FacetPageResponder;
import com.plsqlanalyzer.web.parser.controller.ParserAnalysisController;
import com.plsqlanalyzer.web.service.DbSourceFetcher;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * F6 — the failure flows. The product's whole point in these paths is that a failure is
 * <b>visible and correctly typed</b>: a dead database must not look like "no objects", an expired
 * session must be a re-auth signal rather than a storage outage, and a genuine storage outage must
 * still be the storage error.
 *
 * <p>The last one is the regression this class exists for: the jar listing path wrapped <em>every</em>
 * exception — including the auth-expiry {@link NotAuthenticatedException} — into a 503
 * {@link StorageListingException}, so an expired login looked like an S3 outage and the UI could
 * never offer "sign in again".
 */
@DisplayName("Integration F6: DB down, expired credentials and storage faults are typed correctly")
class EndToEndFailureFlowTest {

    // ── DB down: fail loud ────────────────────────────────────────────────────

    @Test
    @DisplayName("a dead database is a 503, not an empty 200 that reads as 'this schema is empty'")
    void databaseDownIs503NotAnEmptyList() throws Exception {
        PlsqlConfig config = mock(PlsqlConfig.class);
        when(config.getDbUsers()).thenReturn(List.of(new DbUserConfig("ERP", "pw", "demo")));

        DbSourceFetcher dbFetcher = mock(DbSourceFetcher.class);
        when(dbFetcher.getJdbcUrl()).thenReturn("jdbc:oracle:thin:@db.example:1521/XEPDB1");
        when(dbFetcher.getConnection(any(DbUserConfig.class), any(String.class)))
                .thenThrow(new SQLException("connection refused"));

        DatabaseController controller = new DatabaseController(config, dbFetcher,
                mock(com.plsqlanalyzer.analyzer.service.AnalysisService.class),
                mock(com.plsqlanalyzer.web.service.PersistenceService.class),
                mock(com.plsqlanalyzer.web.controller.detail.ObjectResolutionService.class),
                null);

        ResponseEntity<?> response = controller.listObjectsBySchema("ERP", null, null);

        assertEquals(503, response.getStatusCode().value(),
                "a connection failure must not be reported as an empty result set");
    }

    // ── expired credentials: re-auth, not 503 ─────────────────────────────────

    @Test
    @DisplayName("an expired session on the jar listing is a re-auth signal, not a 503 storage fault")
    void expiredCredentialsOnListingSignalReAuth() {
        // The store raises the auth-expiry exception the credential resolver throws when a session
        // has no usable token. The listing must let it through unchanged.
        ArtifactStore store = mock(ArtifactStore.class);
        try {
            when(store.list(any(ArtifactKey.class), anyBoolean()))
                    .thenThrow(new NotAuthenticatedException());
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }

        JarDataPaths paths = new JarDataPaths("data/jar", store);
        PersistenceService persistence = new PersistenceService(paths, store);

        Throwable thrown = assertThrows(Throwable.class, persistence::listJarsStrict);
        assertInstanceOf(NotAuthenticatedException.class, thrown,
                "an expired session must surface as a re-auth signal, but got "
                        + thrown.getClass().getName() + " (the listing wrapped it into a 503)");
    }

    @Test
    @DisplayName("an expired session on the parser listing is a re-auth signal, not a 500")
    void expiredCredentialsOnParserListingSignalReAuth() throws Exception {
        com.plsqlanalyzer.web.parser.service.AnalysisService service =
                mock(com.plsqlanalyzer.web.parser.service.AnalysisService.class);
        when(service.listAnalyses()).thenThrow(new NotAuthenticatedException());

        ParserAnalysisController controller = buildParserController(service);

        Throwable thrown = assertThrows(Throwable.class,
                () -> controller.listAnalyses(null, null, null, null, null, null));
        assertInstanceOf(NotAuthenticatedException.class, thrown,
                "an expired session must surface as a re-auth signal, but got "
                        + thrown.getClass().getName() + " (the listing turned it into a 500)");
    }

    private static ParserAnalysisController buildParserController(
            com.plsqlanalyzer.web.parser.service.AnalysisService service) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Constructor<?> ctor = ParserAnalysisController.class.getConstructors()[0];
            Parameter[] params = ctor.getParameters();
            Object[] args = new Object[params.length];
            for (int i = 0; i < params.length; i++) {
                Class<?> type = params[i].getType();
                if (ObjectMapper.class.equals(type)) args[i] = mapper;
                else if (com.plsqlanalyzer.web.parser.service.AnalysisService.class.equals(type)) args[i] = service;
                else if (FacetPageResponder.class.equals(type)) args[i] = new FacetPageResponder(mapper, 10000);
                else if (type.isPrimitive()) args[i] = (type == boolean.class) ? false : 0;
                else args[i] = mock(type);
            }
            return (ParserAnalysisController) ctor.newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    @DisplayName("a genuine storage outage is still a 503 StorageListingException")
    void genuineStorageOutageIsStill503() {
        ArtifactStore store = mock(ArtifactStore.class);
        try {
            when(store.list(any(ArtifactKey.class), anyBoolean()))
                    .thenThrow(new IOException("S3 unreachable"));
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }

        JarDataPaths paths = new JarDataPaths("data/jar", store);
        PersistenceService persistence = new PersistenceService(paths, store);

        assertThrows(StorageListingException.class, persistence::listJarsStrict,
                "a real storage outage must keep its 503 mapping");
        var response = new ApiExceptionAdvice()
                .onStorageListing(new StorageListingException("S3 unreachable", null));
        assertEquals(503, response.getStatusCode().value());
        assertTrue(response.getBody().get("error").toString().contains("S3 unreachable"));
    }

    @Test
    @DisplayName("the advice types the two failures differently: 401 vs 503")
    void adviceDistinguishesAuthFromStorage() {
        ApiExceptionAdvice advice = new ApiExceptionAdvice();
        assertEquals(401, advice.onNotAuthenticated(new NotAuthenticatedException())
                .getStatusCode().value());
        assertEquals(503, advice.onStorageListing(new StorageListingException("x", null))
                .getStatusCode().value());
    }
}
