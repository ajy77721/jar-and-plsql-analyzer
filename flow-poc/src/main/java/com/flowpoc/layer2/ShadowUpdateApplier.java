/*
 * Decompiled with CFR 0.152.
 * 
 * Could not load the following classes:
 *  org.bson.Document
 */
package com.flowpoc.layer2;

import com.flowpoc.layer2.ShadowFilterMatcher;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.bson.Document;

public class ShadowUpdateApplier {
    public void apply(Document target, Document updateSpec) {
        if (updateSpec == null || updateSpec.isEmpty()) {
            return;
        }
        boolean hasOperators = updateSpec.keySet().stream().anyMatch(k -> k.startsWith("$"));
        if (!hasOperators) {
            Object id = target.get((Object)"_id");
            target.clear();
            if (id != null) {
                target.put("_id", id);
            }
            updateSpec.forEach((k, v) -> {
                if (!k.equals("_id")) {
                    target.put(k, v);
                }
            });
            return;
        }
        this.applySet(target, this.asDoc(updateSpec.get((Object)"$set")));
        this.applyUnset(target, this.asDoc(updateSpec.get((Object)"$unset")));
        this.applyInc(target, this.asDoc(updateSpec.get((Object)"$inc")));
        this.applyMul(target, this.asDoc(updateSpec.get((Object)"$mul")));
        this.applyMin(target, this.asDoc(updateSpec.get((Object)"$min")));
        this.applyMax(target, this.asDoc(updateSpec.get((Object)"$max")));
        this.applyRename(target, this.asDoc(updateSpec.get((Object)"$rename")));
        this.applyPush(target, this.asDoc(updateSpec.get((Object)"$push")));
        this.applyPull(target, this.asDoc(updateSpec.get((Object)"$pull")));
        this.applyPullAll(target, this.asDoc(updateSpec.get((Object)"$pullAll")));
        this.applyAddToSet(target, this.asDoc(updateSpec.get((Object)"$addToSet")));
        this.applyPop(target, this.asDoc(updateSpec.get((Object)"$pop")));
        this.applyCurrentDate(target, this.asDoc(updateSpec.get((Object)"$currentDate")));
    }

    private void applySet(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((k, v) -> this.setNested(t, (String)k, v));
    }

    private void applyUnset(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.keySet().forEach(k -> this.unsetNested(t, (String)k));
    }

    private void applyInc(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((k, v) -> {
            Object cur = this.getNestedOrZero(t, (String)k);
            this.setNested(t, (String)k, this.addNumbers(cur, v));
        });
    }

    private void applyMul(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((k, v) -> {
            Object cur = this.getNestedOrZero(t, (String)k);
            this.setNested(t, (String)k, this.mulNumbers(cur, v));
        });
    }

    private void applyMin(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((k, v) -> {
            Object cur = t.get(k);
            if (cur == null || this.cmp(v, cur) < 0) {
                this.setNested(t, (String)k, v);
            }
        });
    }

    private void applyMax(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((k, v) -> {
            Object cur = t.get(k);
            if (cur == null || this.cmp(v, cur) > 0) {
                this.setNested(t, (String)k, v);
            }
        });
    }

    private void applyRename(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((oldKey, newKey) -> {
            if (t.containsKey(oldKey)) {
                Object val = t.remove(oldKey);
                t.put(String.valueOf(newKey), val);
            }
        });
    }

    private void applyPush(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((k, v) -> {
            Document d;
            List<Object> arr = this.getOrCreateList(t, (String)k);
            if (v instanceof Document && (d = (Document)v).containsKey((Object)"$each")) {
                List each = (List)d.get((Object)"$each");
                if (each != null) {
                    arr.addAll(each);
                }
                this.applySliceAndSort(arr, d);
            } else {
                arr.add(v);
            }
        });
    }

    private void applySliceAndSort(List<Object> arr, Document modifiers) {
        Object sliceSpec;
        Object sortSpec = modifiers.get((Object)"$sort");
        if (sortSpec instanceof Integer) {
            Integer si = (Integer)sortSpec;
            arr.sort((a, b) -> si > 0 ? String.valueOf(a).compareTo(String.valueOf(b)) : String.valueOf(b).compareTo(String.valueOf(a)));
        }
        if ((sliceSpec = modifiers.get((Object)"$slice")) instanceof Number) {
            Number n = (Number)sliceSpec;
            int size = n.intValue();
            if (size >= 0 && arr.size() > size) {
                arr.subList(size, arr.size()).clear();
            } else if (size < 0) {
                int keep = Math.max(0, arr.size() + size);
                arr.subList(0, keep).clear();
            }
        }
    }

    private void applyPull(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((k, condition) -> {
            Object cur = t.get(k);
            if (!(cur instanceof List)) {
                return;
            }
            List list = (List)cur;
            ArrayList<Object> mutable = new ArrayList<Object>(list);
            mutable.removeIf(item -> {
                if (condition instanceof Document) {
                    Document document;
                    Document cond = (Document)condition;
                    if (item instanceof Document) {
                        Document d;
                        document = d = (Document)item;
                    } else if (item instanceof Map) {
                        Map m = (Map)item;
                        document = new Document(m);
                    } else {
                        document = null;
                    }
                    Document itemDoc = document;
                    return itemDoc != null && new ShadowFilterMatcher().matches(itemDoc, cond);
                }
                return Objects.equals(item, condition) || item != null && item.toString().equals(String.valueOf(condition));
            });
            t.put(k, mutable);
        });
    }

    private void applyPullAll(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((k, v) -> {
            if (!(v instanceof List)) {
                return;
            }
            List toRemove = (List)v;
            Object cur = t.get(k);
            if (!(cur instanceof List)) {
                return;
            }
            List list = (List)cur;
            ArrayList<Object> mutable = new ArrayList<Object>(list);
            mutable.removeIf(item -> toRemove.stream().anyMatch(r -> Objects.equals(item, r) || item != null && item.toString().equals(String.valueOf(r))));
            t.put(k, mutable);
        });
    }

    private void applyAddToSet(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((k, v) -> {
            Document d;
            List toAdd;
            List<Object> arr = this.getOrCreateList(t, (String)k);
            List list = toAdd = v instanceof Document && (d = (Document)v).containsKey((Object)"$each") ? (List)d.get((Object)"$each") : List.of(v);
            if (toAdd == null) {
                return;
            }
            for (Object item : toAdd) {
                boolean exists = arr.stream().anyMatch(a -> Objects.equals(a, item) || a != null && a.toString().equals(String.valueOf(item)));
                if (exists) continue;
                arr.add(item);
            }
        });
    }

    private void applyPop(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        spec.forEach((k, v) -> {
            int dir;
            List list;
            Object cur = t.get(k);
            if (!(cur instanceof List) || (list = (List)cur).isEmpty()) {
                return;
            }
            ArrayList mutable = new ArrayList(list);
            if (v instanceof Number) {
                Number n = (Number)v;
                dir = n.intValue();
            } else {
                dir = 1;
            }
            if (dir == -1) {
                mutable.remove(0);
            } else {
                mutable.remove(mutable.size() - 1);
            }
            t.put(k, mutable);
        });
    }

    private void applyCurrentDate(Document t, Document spec) {
        if (spec == null) {
            return;
        }
        Date now = new Date();
        spec.forEach((k, v) -> t.put(k, (Object)now));
    }

    private void setNested(Document doc, String path, Object value) {
        Document nested;
        if (!path.contains(".")) {
            doc.put(path, value);
            return;
        }
        String[] parts = path.split("\\.", 2);
        Object child = doc.get((Object)parts[0]);
        if (child instanceof Document) {
            Document d;
            nested = d = (Document)child;
        } else if (child instanceof Map) {
            Map m = (Map)child;
            nested = new Document(m);
            doc.put(parts[0], (Object)nested);
        } else {
            nested = new Document();
            doc.put(parts[0], (Object)nested);
        }
        this.setNested(nested, parts[1], value);
    }

    private void unsetNested(Document doc, String path) {
        if (!path.contains(".")) {
            doc.remove((Object)path);
            return;
        }
        String[] parts = path.split("\\.", 2);
        Object child = doc.get((Object)parts[0]);
        if (child instanceof Document) {
            Document nested = (Document)child;
            this.unsetNested(nested, parts[1]);
        }
    }

    private Object getNestedOrZero(Document doc, String path) {
        if (!path.contains(".")) {
            Object v = doc.get((Object)path);
            return v != null ? v : Integer.valueOf(0);
        }
        String[] parts = path.split("\\.", 2);
        Object child = doc.get((Object)parts[0]);
        if (child instanceof Document) {
            Document nested = (Document)child;
            return this.getNestedOrZero(nested, parts[1]);
        }
        return 0;
    }

    private List<Object> getOrCreateList(Document doc, String key) {
        Object cur = doc.get((Object)key);
        if (cur instanceof List) {
            List list = (List)cur;
            ArrayList<Object> mutable = new ArrayList<Object>(list);
            doc.put(key, mutable);
            return mutable;
        }
        ArrayList<Object> newList = new ArrayList<Object>();
        doc.put(key, newList);
        return newList;
    }

    private Object addNumbers(Object a, Object b) {
        double da = this.toDouble(a);
        double db = this.toDouble(b);
        double result = da + db;
        if (a instanceof Long || b instanceof Long) {
            return (long)result;
        }
        if (a instanceof Integer || b instanceof Integer) {
            return (int)result;
        }
        return result;
    }

    private Object mulNumbers(Object a, Object b) {
        double result = this.toDouble(a) * this.toDouble(b);
        if (a instanceof Long || b instanceof Long) {
            return (long)result;
        }
        if (a instanceof Integer || b instanceof Integer) {
            return (int)result;
        }
        return result;
    }

    private double toDouble(Object v) {
        if (v instanceof Number) {
            Number n = (Number)v;
            return n.doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(v));
        }
        catch (Exception e) {
            return 0.0;
        }
    }

    private int cmp(Object a, Object b) {
        if (a instanceof Number) {
            Number na = (Number)a;
            if (b instanceof Number) {
                Number nb = (Number)b;
                return Double.compare(na.doubleValue(), nb.doubleValue());
            }
        }
        if (a instanceof Comparable) {
            Comparable ca = (Comparable)a;
            return ca.compareTo(b);
        }
        return a.toString().compareTo(b.toString());
    }

    private Document asDoc(Object obj) {
        if (obj instanceof Document) {
            Document d = (Document)obj;
            return d;
        }
        if (obj instanceof Map) {
            Map m = (Map)obj;
            return new Document(m);
        }
        return null;
    }
}

