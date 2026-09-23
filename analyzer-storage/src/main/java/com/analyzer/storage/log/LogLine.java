package com.analyzer.storage.log;

/**
 * One line of an application log, addressed by a monotonic sequence number.
 *
 * @param seq     1-based line ordinal within the current log generation. The cursor key: a client
 *                that has seen {@code seq} asks for {@code sinceSeq=seq} and receives only what
 *                arrived after it.
 * @param ts      the timestamp prefix, when the line carries one; empty for a continuation line
 *                such as a stack-trace frame
 * @param level   {@code ERROR}/{@code WARN}/{@code INFO}/{@code DEBUG}/{@code TRACE}, or empty
 * @param message the line exactly as written, timestamp and level included.
 *                <p>Deliberately the whole line rather than the message field alone: the log
 *                viewers render a {@code <pre>} of raw lines, and returning a reassembled subset
 *                would change what is on screen. The parsed {@code level} exists so the filter can
 *                move to the server, not so the client can rebuild the line.
 */
public record LogLine(long seq, String ts, String level, String message) {
}
