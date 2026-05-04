package com.plsqlanalyzer.web.service;

import com.plsqlanalyzer.analyzer.graph.CallGraph;
import com.plsqlanalyzer.analyzer.model.AnalysisResult;
import com.plsqlanalyzer.analyzer.model.CursorOperationSummary;
import com.plsqlanalyzer.analyzer.model.JoinOperationSummary;
import com.plsqlanalyzer.analyzer.model.SequenceOperationSummary;
import com.plsqlanalyzer.analyzer.model.TableOperationSummary;
import com.plsqlanalyzer.parser.model.PlsqlUnit;
import com.plsqlanalyzer.parser.service.OracleDictionaryService.TableMetadata;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lazy-loading wrapper around AnalysisResult. Sections are loaded from
 * individual split files on first access rather than loading the entire
 * 27-45MB analysis.json at once.
 */
public class LazyAnalysisResult extends AnalysisResult {

    private final PersistenceService persistence;
    private final Path folder;

    private volatile boolean graphLoaded;
    private volatile boolean sourceLoaded;
    private volatile boolean opsLoaded;
    private volatile boolean unitsLoaded;
    private volatile boolean tableMetaLoaded;

    private final Object graphLock = new Object();
    private final Object sourceLock = new Object();
    private final Object opsLock = new Object();
    private final Object unitsLock = new Object();
    private final Object tableMetaLock = new Object();

    private Map<String, Object> metadataCache;

    public LazyAnalysisResult(String name, Path folder, PersistenceService persistence) {
        this.folder = folder;
        this.persistence = persistence;
        setName(name);
    }

    public void setMetadataCache(Map<String, Object> meta) {
        this.metadataCache = meta;
    }

    public Map<String, Object> getMetadataCache() {
        return metadataCache;
    }

    public boolean isGraphLoaded() { return graphLoaded; }
    public boolean isSourceLoaded() { return sourceLoaded; }
    public boolean isOpsLoaded() { return opsLoaded; }
    public boolean isUnitsLoaded() { return unitsLoaded; }
    public boolean isTableMetaLoaded() { return tableMetaLoaded; }

    public Path getFolder() { return folder; }

    public void markFullyLoaded() {
        graphLoaded = sourceLoaded = opsLoaded = unitsLoaded = tableMetaLoaded = true;
    }

    // ---- Override getters to trigger lazy load ----

    @Override
    public CallGraph getCallGraph() {
        ensureGraphLoaded();
        return super.getCallGraph();
    }

    @Override
    public Map<String, List<String>> getSourceMap() {
        ensureSourceLoaded();
        return super.getSourceMap();
    }

    @Override
    public Map<String, TableOperationSummary> getTableOperations() {
        ensureOpsLoaded();
        return super.getTableOperations();
    }

    @Override
    public Map<String, SequenceOperationSummary> getSequenceOperations() {
        ensureOpsLoaded();
        return super.getSequenceOperations();
    }

    @Override
    public Map<String, JoinOperationSummary> getJoinOperations() {
        ensureOpsLoaded();
        return super.getJoinOperations();
    }

    @Override
    public Map<String, CursorOperationSummary> getCursorOperations() {
        ensureOpsLoaded();
        return super.getCursorOperations();
    }

    @Override
    public List<PlsqlUnit> getUnits() {
        ensureUnitsLoaded();
        return super.getUnits();
    }

    @Override
    public Map<String, TableMetadata> getTableMetadata() {
        ensureTableMetaLoaded();
        return super.getTableMetadata();
    }

    // ---- Override setters to mark sections loaded ----

    @Override
    public void setCallGraph(CallGraph cg) {
        super.setCallGraph(cg);
        graphLoaded = true;
    }

    @Override
    public void setSourceMap(Map<String, List<String>> m) {
        super.setSourceMap(m);
        sourceLoaded = true;
    }

    @Override
    public void setTableOperations(Map<String, TableOperationSummary> m) {
        super.setTableOperations(m);
        opsLoaded = true;
    }

    @Override
    public void setSequenceOperations(Map<String, SequenceOperationSummary> m) {
        super.setSequenceOperations(m);
        opsLoaded = true;
    }

    @Override
    public void setJoinOperations(Map<String, JoinOperationSummary> m) {
        super.setJoinOperations(m);
        opsLoaded = true;
    }

    @Override
    public void setCursorOperations(Map<String, CursorOperationSummary> m) {
        super.setCursorOperations(m);
        opsLoaded = true;
    }

    @Override
    public void setUnits(List<PlsqlUnit> u) {
        super.setUnits(u);
        unitsLoaded = true;
    }

    @Override
    public void setTableMetadata(Map<String, TableMetadata> m) {
        super.setTableMetadata(m);
        tableMetaLoaded = true;
    }

    // ---- Lazy-load triggers ----

    private void ensureGraphLoaded() {
        if (graphLoaded) return;
        synchronized (graphLock) {
            if (graphLoaded) return;
            persistence.loadSection(this, folder, PersistenceService.AnalysisSection.GRAPH);
            graphLoaded = true;
        }
    }

    private void ensureSourceLoaded() {
        if (sourceLoaded) return;
        synchronized (sourceLock) {
            if (sourceLoaded) return;
            persistence.loadSection(this, folder, PersistenceService.AnalysisSection.SOURCE);
            sourceLoaded = true;
        }
    }

    private void ensureOpsLoaded() {
        if (opsLoaded) return;
        synchronized (opsLock) {
            if (opsLoaded) return;
            persistence.loadSection(this, folder, PersistenceService.AnalysisSection.OPS);
            opsLoaded = true;
        }
    }

    private void ensureUnitsLoaded() {
        if (unitsLoaded) return;
        synchronized (unitsLock) {
            if (unitsLoaded) return;
            persistence.loadSection(this, folder, PersistenceService.AnalysisSection.UNITS);
            unitsLoaded = true;
        }
    }

    private void ensureTableMetaLoaded() {
        if (tableMetaLoaded) return;
        synchronized (tableMetaLock) {
            if (tableMetaLoaded) return;
            persistence.loadSection(this, folder, PersistenceService.AnalysisSection.TABLE_META);
            tableMetaLoaded = true;
        }
    }
}
