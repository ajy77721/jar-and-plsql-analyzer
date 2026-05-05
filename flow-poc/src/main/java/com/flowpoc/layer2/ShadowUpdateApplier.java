package com.flowpoc.layer2;

import org.bson.Document;

import java.util.*;

/**
 * Applies MongoDB update operator documents to an in-memory Document.
 *
 * Supported operators:
 *   $set        – set field values
 *   $unset      – remove fields
 *   $inc        – increment numeric fields
 *   $mul        – multiply numeric fields
 *   $min/$max   – conditional set
 *   $rename     – rename a field
 *   $push       – append to array (with $each, $slice, $sort)
 *   $pull       – remove matching elements from array
 *   $pullAll    – remove listed values from array
 *   $addToSet   – push only if not already present
 *   $pop        – remove first (-1) or last (1) array element
 *   $currentDate – set to current date/timestamp
 *
 * If the update document contains no $ operators it is treated as a full
 * replacement (all fields replaced, _id preserved).
 */
public class ShadowUpdateApplier {

    public void apply(Document target, Document updateSpec) {
        if (updateSpec == null || updateSpec.isEmpty()) return;

        boolean hasOperators = updateSpec.keySet().stream().anyMatch(k -> k.startsWith("$"));
        if (!hasOperators) {
            // Full replacement — preserve _id
            Object id = target.get("_id");
            target.clear();
            if (id != null) target.put("_id", id);
            updateSpec.forEach((k, v) -> { if (!k.equals("_id")) target.put(k, v); });
            return;
        }

        applySet(target,         asDoc(updateSpec.get("$set")));
        applyUnset(target,       asDoc(updateSpec.get("$unset")));
        applyInc(target,         asDoc(updateSpec.get("$inc")));
        applyMul(target,         asDoc(updateSpec.get("$mul")));
        applyMin(target,         asDoc(updateSpec.get("$min")));
        applyMax(target,         asDoc(updateSpec.get("$max")));
        applyRename(target,      asDoc(updateSpec.get("$rename")));
        applyPush(target,        asDoc(updateSpec.get("$push")));
        applyPull(target,        asDoc(updateSpec.get("$pull")));
        applyPullAll(target,     asDoc(updateSpec.get("$pullAll")));
        applyAddToSet(target,    asDoc(updateSpec.get("$addToSet")));
        applyPop(target,         asDoc(updateSpec.get("$pop")));
        applyCurrentDate(target, asDoc(updateSpec.get("$currentDate")));
    }

    // ── Operators ─────────────────────────────────────────────────────────────

    private void applySet(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((k, v) -> setNested(t, k, v));
    }

    private void applyUnset(Document t, Document spec) {
        if (spec == null) return;
        spec.keySet().forEach(k -> unsetNested(t, k));
    }

    private void applyInc(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((k, v) -> {
            Object cur = getNestedOrZero(t, k);
            setNested(t, k, addNumbers(cur, v));
        });
    }

    private void applyMul(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((k, v) -> {
            Object cur = getNestedOrZero(t, k);
            setNested(t, k, mulNumbers(cur, v));
        });
    }

    private void applyMin(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((k, v) -> {
            Object cur = t.get(k);
            if (cur == null || cmp(v, cur) < 0) setNested(t, k, v);
        });
    }

    private void applyMax(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((k, v) -> {
            Object cur = t.get(k);
            if (cur == null || cmp(v, cur) > 0) setNested(t, k, v);
        });
    }

    private void applyRename(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((oldKey, newKey) -> {
            if (t.containsKey(oldKey)) {
                Object val = t.remove(oldKey);
                t.put(String.valueOf(newKey), val);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void applyPush(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((k, v) -> {
            List<Object> arr = getOrCreateList(t, k);
            if (v instanceof Document d && d.containsKey("$each")) {
                List<?> each = (List<?>) d.get("$each");
                if (each != null) arr.addAll(each);
                applySliceAndSort(arr, d);
            } else {
                arr.add(v);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void applySliceAndSort(List<Object> arr, Document modifiers) {
        Object sortSpec = modifiers.get("$sort");
        if (sortSpec instanceof Integer si) {
            arr.sort((a, b) -> si > 0
                    ? String.valueOf(a).compareTo(String.valueOf(b))
                    : String.valueOf(b).compareTo(String.valueOf(a)));
        }
        Object sliceSpec = modifiers.get("$slice");
        if (sliceSpec instanceof Number n) {
            int size = n.intValue();
            if (size >= 0 && arr.size() > size) {
                arr.subList(size, arr.size()).clear();
            } else if (size < 0) {
                int keep = Math.max(0, arr.size() + size);
                arr.subList(0, keep).clear();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void applyPull(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((k, condition) -> {
            Object cur = t.get(k);
            if (!(cur instanceof List<?> list)) return;
            List<Object> mutable = new ArrayList<>(list);
            mutable.removeIf(item -> {
                if (condition instanceof Document cond) {
                    Document itemDoc = item instanceof Document d ? d
                            : item instanceof Map<?,?> m ? new Document((Map<String,Object>) m) : null;
                    return itemDoc != null && new ShadowFilterMatcher().matches(itemDoc, cond);
                }
                return Objects.equals(item, condition) ||
                       (item != null && item.toString().equals(String.valueOf(condition)));
            });
            t.put(k, mutable);
        });
    }

    @SuppressWarnings("unchecked")
    private void applyPullAll(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((k, v) -> {
            if (!(v instanceof List<?> toRemove)) return;
            Object cur = t.get(k);
            if (!(cur instanceof List<?> list)) return;
            List<Object> mutable = new ArrayList<>(list);
            mutable.removeIf(item -> toRemove.stream()
                    .anyMatch(r -> Objects.equals(item, r)
                                  || (item != null && item.toString().equals(String.valueOf(r)))));
            t.put(k, mutable);
        });
    }

    @SuppressWarnings("unchecked")
    private void applyAddToSet(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((k, v) -> {
            List<Object> arr = getOrCreateList(t, k);
            List<?> toAdd = (v instanceof Document d && d.containsKey("$each"))
                    ? (List<?>) d.get("$each")
                    : List.of(v);
            if (toAdd == null) return;
            for (Object item : toAdd) {
                boolean exists = arr.stream().anyMatch(a -> Objects.equals(a, item)
                        || (a != null && a.toString().equals(String.valueOf(item))));
                if (!exists) arr.add(item);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void applyPop(Document t, Document spec) {
        if (spec == null) return;
        spec.forEach((k, v) -> {
            Object cur = t.get(k);
            if (!(cur instanceof List<?> list) || list.isEmpty()) return;
            List<Object> mutable = new ArrayList<>(list);
            int dir = (v instanceof Number n) ? n.intValue() : 1;
            if (dir == -1) mutable.remove(0);
            else           mutable.remove(mutable.size() - 1);
            t.put(k, mutable);
        });
    }

    private void applyCurrentDate(Document t, Document spec) {
        if (spec == null) return;
        Date now = new Date();
        spec.forEach((k, v) -> t.put(k, now));
    }

    // ── Nested path set/get ───────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void setNested(Document doc, String path, Object value) {
        if (!path.contains(".")) { doc.put(path, value); return; }
        String[] parts = path.split("\\.", 2);
        Object child = doc.get(parts[0]);
        Document nested;
        if (child instanceof Document d) {
            nested = d;
        } else if (child instanceof Map<?,?> m) {
            nested = new Document((Map<String,Object>) m);
            doc.put(parts[0], nested);
        } else {
            nested = new Document();
            doc.put(parts[0], nested);
        }
        setNested(nested, parts[1], value);
    }

    private void unsetNested(Document doc, String path) {
        if (!path.contains(".")) { doc.remove(path); return; }
        String[] parts = path.split("\\.", 2);
        Object child = doc.get(parts[0]);
        if (child instanceof Document nested) unsetNested(nested, parts[1]);
    }

    private Object getNestedOrZero(Document doc, String path) {
        if (!path.contains(".")) {
            Object v = doc.get(path);
            return v != null ? v : 0;
        }
        String[] parts = path.split("\\.", 2);
        Object child = doc.get(parts[0]);
        if (child instanceof Document nested) return getNestedOrZero(nested, parts[1]);
        return 0;
    }

    @SuppressWarnings("unchecked")
    private List<Object> getOrCreateList(Document doc, String key) {
        Object cur = doc.get(key);
        if (cur instanceof List<?> list) {
            // Always return a mutable copy — the stored list may be immutable (e.g. List.of())
            List<Object> mutable = new ArrayList<>((List<Object>) list);
            doc.put(key, mutable);
            return mutable;
        }
        List<Object> newList = new ArrayList<>();
        doc.put(key, newList);
        return newList;
    }

    // ── Arithmetic ────────────────────────────────────────────────────────────

    private Object addNumbers(Object a, Object b) {
        double da = toDouble(a), db = toDouble(b);
        double result = da + db;
        if (a instanceof Long   || b instanceof Long)   return (long) result;
        if (a instanceof Integer || b instanceof Integer) return (int) result;
        return result;
    }

    private Object mulNumbers(Object a, Object b) {
        double result = toDouble(a) * toDouble(b);
        if (a instanceof Long   || b instanceof Long)   return (long) result;
        if (a instanceof Integer || b instanceof Integer) return (int) result;
        return result;
    }

    private double toDouble(Object v) {
        if (v instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(String.valueOf(v)); } catch (Exception e) { return 0; }
    }

    @SuppressWarnings("unchecked")
    private int cmp(Object a, Object b) {
        if (a instanceof Number na && b instanceof Number nb)
            return Double.compare(na.doubleValue(), nb.doubleValue());
        if (a instanceof Comparable ca) return ca.compareTo(b);
        return a.toString().compareTo(b.toString());
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Document asDoc(Object obj) {
        if (obj instanceof Document d) return d;
        if (obj instanceof Map<?,?> m)  return new Document((Map<String,Object>) m);
        return null;
    }
}
