package com.analyzer.storage.log;

import com.analyzer.storage.page.PageInfo;

import java.util.List;

/**
 * The JSON body of a log-tail response — the standard {@code {items, page}} envelope with the two
 * append-only-stream extras PLAN-08 §5.3 defines beside it.
 *
 * <pre>
 * { "items": [ {"seq":1844,"ts":"…","level":"INFO","message":"…"}, … ],
 *   "page": { "nextCursor": null, "hasMore": false, "total": -1, "totalIsExact": false,
 *             "limit": 200, "sort": "SEQ_ASC" },
 *   "watermark": 1861, "reset": false, "fromSeq": 1, "fileSize": 9821334 }
 * </pre>
 *
 * <p>Flat rather than nesting a {@code Page} inside, because the client reads {@code items} and
 * {@code page} through the same {@code PA.normalizePage} shim as every other converted endpoint —
 * an extra level of nesting here would be one endpoint the shim does not cover.
 *
 * <p>{@code total} is {@code -1}: counting the matching lines in a log means reading the log, which
 * is the cost this endpoint exists to avoid. {@code watermark} is the honest substitute — it tells
 * the client how far behind it is without sending it anything.
 */
public record LogTailBody(List<LogLine> items, PageInfo page, long watermark, boolean reset,
                          long fromSeq, long fileSize) {

    public static LogTailBody of(LogTail tail) {
        return new LogTailBody(tail.page().items(), tail.page().page(), tail.watermark(),
                tail.reset(), tail.fromSeq(), tail.fileSize());
    }
}
