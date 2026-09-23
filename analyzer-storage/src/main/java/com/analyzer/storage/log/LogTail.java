package com.analyzer.storage.log;

import com.analyzer.storage.page.Page;

/**
 * The answer to a log-tail query: a bounded page of lines, plus the two facts a polling client
 * needs in order to stay honest about what it has not seen.
 *
 * @param page      the lines, in {@code seq} order, with the standard paging envelope
 * @param watermark the highest {@code seq} that exists right now. The client can render
 *                  "17 new lines" without fetching them, and can tell that it has fallen behind.
 * @param reset     true when the caller's {@code sinceSeq} no longer exists — the log rotated or
 *                  was truncated underneath it. The client must clear its buffer and re-render
 *                  from {@code fromSeq}. Never a silent gap: a poller that missed a rotation would
 *                  otherwise append new lines onto stale ones and show a log that never existed.
 * @param fromSeq   the lowest {@code seq} still retained; the floor to restart from after a reset
 * @param fileSize  size of the log in bytes, for the viewers' existing "N lines | N KB" caption
 */
public record LogTail(Page<LogLine> page, long watermark, boolean reset, long fromSeq,
                      long fileSize) {
}
