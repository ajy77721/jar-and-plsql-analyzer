package com.plsql.parser;

import com.plsql.parser.flow.TriggerDiscoverer;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TriggerSequenceExtractionTest {

    private final TriggerDiscoverer discoverer = new TriggerDiscoverer(null, null);

    @Test
    void extractsSimpleNextval() {
        String body = "BEGIN :NEW.ID := MY_SEQ.NEXTVAL; END;";
        List<Map<String, Object>> seqs = discoverer.extractSequencesFromBody(body);
        assertEquals(1, seqs.size());
        assertEquals("MY_SEQ", seqs.get(0).get("sequenceName"));
        assertEquals("NEXTVAL", seqs.get(0).get("operation"));
        assertNull(seqs.get(0).get("schema"));
    }

    @Test
    void extractsSchemaQualifiedNextval() {
        String body = "BEGIN :NEW.MAIL_ID := CUSTOMER.SEQ_SABJ_MAIL.NEXTVAL; END;";
        List<Map<String, Object>> seqs = discoverer.extractSequencesFromBody(body);
        assertEquals(1, seqs.size());
        assertEquals("SEQ_SABJ_MAIL", seqs.get(0).get("sequenceName"));
        assertEquals("CUSTOMER", seqs.get(0).get("schema"));
    }

    @Test
    void extractsMultipleSequences() {
        String body = "BEGIN :NEW.ID := SEQ_A.NEXTVAL; :NEW.AID := SEQ_B.NEXTVAL; v := SEQ_A.CURRVAL; END;";
        List<Map<String, Object>> seqs = discoverer.extractSequencesFromBody(body);
        assertEquals(3, seqs.size());
    }

    @Test
    void deduplicatesSameSequenceSameOp() {
        String body = "BEGIN IF x THEN :NEW.ID := MY_SEQ.NEXTVAL; ELSE :NEW.ID := MY_SEQ.NEXTVAL; END IF; END;";
        List<Map<String, Object>> seqs = discoverer.extractSequencesFromBody(body);
        assertEquals(1, seqs.size());
    }

    @Test
    void returnsEmptyForNullBody() {
        assertTrue(discoverer.extractSequencesFromBody(null).isEmpty());
    }

    @Test
    void returnsEmptyForBodyWithoutSequences() {
        String body = "BEGIN :NEW.CREATED_DATE := SYSDATE; END;";
        assertTrue(discoverer.extractSequencesFromBody(body).isEmpty());
    }

    @Test
    void extractsInsertFromTriggerBody() {
        String body = "BEGIN INSERT INTO AUDIT_LOG (X) VALUES (1); END;";
        List<Map<String, Object>> ops = discoverer.extractDmlFromBody(body, Collections.emptyMap());
        assertEquals(1, ops.size());
        assertEquals("INSERT", ops.get(0).get("operation"));
        assertEquals("AUDIT_LOG", ops.get(0).get("tableName"));
    }

    @Test
    void combinedSequenceAndDml() {
        String body = "BEGIN :NEW.ID := MY_SEQ.NEXTVAL; INSERT INTO AUDIT_LOG (ID) VALUES (:NEW.ID); END;";
        assertEquals(1, discoverer.extractSequencesFromBody(body).size());
        assertEquals(1, discoverer.extractDmlFromBody(body, Collections.emptyMap()).size());
    }
}
