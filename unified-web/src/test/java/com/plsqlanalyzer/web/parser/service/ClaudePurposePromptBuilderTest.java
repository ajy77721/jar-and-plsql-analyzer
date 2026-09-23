package com.plsqlanalyzer.web.parser.service;

import com.plsqlanalyzer.web.parser.service.ClaudePurposeSummaryModels.PurposeChunk;
import com.plsqlanalyzer.web.parser.service.ClaudePurposeSummaryModels.PurposeTarget;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ClaudePurposePromptBuilderTest {

    @Test
    void buildPrompt_includesEveryTargetIdentityAndContext_notJustSource() {
        PurposeTarget t1 = new PurposeTarget();
        t1.nodeId = "N1";
        t1.name = "ERP_USP_WRITE_AUDIT_TRAIL";
        t1.schema = "ERP";
        t1.objectType = "PROCEDURE";
        t1.sourceSnippet = "BEGIN INSERT INTO AUDIT_LOG VALUES(1); END;";
        t1.tableSummaries.add("AUDIT_LOG(INSERT)");
        t1.callSummaries.add("ERP_USP_HELPER");

        PurposeTarget t2 = new PurposeTarget();
        t2.nodeId = "N2";
        t2.name = "ERP_USP_NOOP";
        t2.sourceSnippet = "BEGIN NULL; END;";

        PurposeChunk chunk = new PurposeChunk();
        chunk.id = "pchunk_1";
        chunk.targets.add(t1);
        chunk.targets.add(t2);

        String prompt = new ClaudePurposePromptBuilder().buildPrompt(chunk);

        assertTrue(prompt.contains("N1"), "prompt must reference target 1's nodeId");
        assertTrue(prompt.contains("N2"), "prompt must reference target 2's nodeId");
        assertTrue(prompt.contains("ERP_USP_WRITE_AUDIT_TRAIL"));
        assertTrue(prompt.contains("AUDIT_LOG(INSERT)"), "known table ops must be passed as context");
        assertTrue(prompt.contains("ERP_USP_HELPER"), "known calls-out must be passed as context");
        assertTrue(prompt.contains("BEGIN INSERT INTO AUDIT_LOG"), "actual source must be included");
        assertTrue(prompt.contains("\"purpose\""), "response format must ask for a purpose field");
        assertTrue(prompt.contains("\"perStatement\""), "response format must offer optional perStatement");
        // Not a verification prompt: must not ask for CONFIRMED/REMOVED/NEW table-op statuses.
        assertFalse(prompt.contains("REMOVED"));
    }

    @Test
    void buildPrompt_omitsOptionalSections_whenNoTablesOrCalls() {
        PurposeTarget t = new PurposeTarget();
        t.nodeId = "N1";
        t.name = "X";
        t.sourceSnippet = "BEGIN NULL; END;";

        PurposeChunk chunk = new PurposeChunk();
        chunk.id = "pchunk_1";
        chunk.targets.add(t);

        String prompt = new ClaudePurposePromptBuilder().buildPrompt(chunk);
        assertFalse(prompt.contains("Known table operations"));
        assertFalse(prompt.contains("Known calls out"));
    }
}
