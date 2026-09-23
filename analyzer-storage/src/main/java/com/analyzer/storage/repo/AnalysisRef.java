package com.analyzer.storage.repo;

import com.analyzer.storage.ArtifactKey;

/**
 * One analysis, as the two facts only storage can supply: its identity and where its index
 * document lives.
 *
 * <p>Not a summary. Interpreting the index document into "12 procedures, 3 errors, ORACLE" is the
 * caller's domain model; this is the reference the caller opens. Keeping the split here is what
 * lets the Mongo implementation answer {@code analyses} from a collection without also having to
 * reproduce a web model's field-by-field shape.
 *
 * @param name         the analysis identity — for PL/SQL the {@code '~'}-joined
 *                     {@code DB~PROJECT~ENV~OBJECT}, which is what every other endpoint's path
 *                     variable carries
 * @param index        key of the analysis's index document
 * @param lastModified index-document mtime in millis, or -1 if unknown. The ordering key: it used
 *                     to come from the directory mtime, but object stores have no directories, and
 *                     the index is written last in every run so the ordering is the same
 */
public record AnalysisRef(String name, ArtifactKey index, long lastModified) {
}
