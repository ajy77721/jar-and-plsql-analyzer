package com.plsqlanalyzer.web.promptconfig;

import com.jaranalyzer.service.PromptTemplates;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.Map;

/**
 * Bridges the unified-web {@link PromptConfigService} (which owns the editable prompt
 * configuration) to the jar-analyzer-core {@link PromptTemplates} (which builds the Java
 * Claude prompts in a module that cannot depend on unified-web).
 *
 * <p>On startup it registers a {@link PromptTemplates.PromptExtrasProvider} that resolves the
 * additive global + per-analysis CONTEXT / INSTRUCTIONS blocks. This is additive only — it
 * never rewrites the base template text. The current analysis (jar) name is set by the Java
 * Claude services at their enrichment/correction entry points.
 */
@Component
public class PromptExtrasBridge {

    private final PromptConfigService promptConfig;

    public PromptExtrasBridge(PromptConfigService promptConfig) {
        this.promptConfig = promptConfig;
    }

    @PostConstruct
    public void register() {
        PromptTemplates.setExtrasProvider(new PromptTemplates.PromptExtrasProvider() {
            @Override
            public String contextBlock(String analysisName) {
                return combine("globalContext", "context", analysisName);
            }

            @Override
            public String appendBlock(String analysisName) {
                return combine("globalAppend", "append", analysisName);
            }
        });
    }

    /** Combine the global extra with the per-analysis extra (both optional). */
    private String combine(String globalKey, String analysisKey, String analysisName) {
        String global = "";
        try {
            Map<String, String> extras = promptConfig.getExtras();
            if (extras != null) global = nz(extras.get(globalKey));
        } catch (Exception ignored) { }

        String perAnalysis = "";
        if (analysisName != null && !analysisName.isBlank()) {
            try {
                Map<String, Object> a = promptConfig.getAnalysis(analysisName);
                if (a != null && a.get(analysisKey) != null) {
                    perAnalysis = nz(a.get(analysisKey).toString());
                }
            } catch (Exception ignored) { }
        }

        String g = global.trim(), p = perAnalysis.trim();
        if (g.isEmpty()) return p;
        if (p.isEmpty()) return g;
        return g + "\n" + p;
    }

    private static String nz(String s) { return s == null ? "" : s; }
}
