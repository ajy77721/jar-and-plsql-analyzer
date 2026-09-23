package com.analyzer.storage.entity;

import java.util.List;

/**
 * One call from a flow node to another object — PLAN-04's {@code flow_edge}.
 *
 * <p><b>Sourced from {@code api/nodes/*.json}'s {@code calls[]}, not from {@code edges/*.json}</b>,
 * and that is a mapping decision, not a convenience. PLAN-11 §3 lists {@code calls[].type} (582
 * files) as the origin of {@code flow_edge.call_type}; the {@code edges/} shards carry
 * {@code from/to/line/depth} but <em>no</em> {@code type}, so mapping from them would produce edges
 * with a null call type and no way to recover it. Reading {@code calls[]} also means the whole
 * flow-node spine comes out of one streaming pass over one artifact family rather than two passes
 * that have to be reconciled.
 *
 * <p>{@code depth} is deliberately absent: it is a property of the crawl, not of the edge, and
 * PLAN-11 rule 5 forbids keying on it. Callers that want depth read it from the node.
 *
 * @param lines every line the call appears on. An array field, not a row per line and not an
 *              ordinal-keyed child: the lines are a set of positions in the source, and nothing
 *              joins to an individual one
 */
public record FlowEdgeRow(String analysis,
                          String fromNodeId,
                          String toNodeId,
                          String toSchema,
                          String toObject,
                          String packageName,
                          String callType,
                          Integer argCount,
                          Integer count,
                          List<Integer> lines) {
}
