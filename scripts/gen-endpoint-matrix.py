#!/usr/bin/env python3
"""
gen-endpoint-matrix.py — build the endpoint x parameter-combination matrix for the
amp-analyzer web app, straight from the controller SOURCE (the source of truth).

The checked-in docs/api-render-map.json is known to be incomplete/stale (247 live + 42
unused at last audit) — this generator deliberately re-derives the inventory from the
@RestController classes under unified-web/src/main so the sweep covers whatever the code
actually exposes, including endpoints the render map never recorded.

What it emits
-------------
A flat TSV matrix, one row per (endpoint, parameter combination):

    method  path_template  query  variant  stream  mutating  controller

  path_template : e.g. /api/parser/analyses/{name}/procedures  ({..} = runtime-substituted id)
  query         : the fully built query string (no leading '?'), may be empty
  variant       : "baseline" for the all-defaults row, else "param=value" naming the ONE
                  parameter this row varies away from the baseline (1-factor-at-a-time)
  stream        : 1 for SSE / text-event-stream endpoints (long-lived; sweep skips by default)
  mutating      : 1 for anything that is not a body-less GET/HEAD (POST/PUT/DELETE/PATCH)
  controller    : the owning Java class, for triage

The 1-factor-at-a-time shape (baseline + one row per individual param variant) is what
keeps the matrix bounded and readable: a full cartesian product over the parser controller
alone would be tens of thousands of rows, and 90% of them 400 for reasons that say nothing
about endpoint health. Every @RequestParam value we consider meaningful still appears.

Discovery
---------
With --discover (the default when a cookie+base-url are supplied) it makes read-only calls
to the staging app to fetch REAL ids — analysis names, jar ids, war ids, a procedure/node
id, a table name — and substitutes them into {path} templates so the sweep hits real data
rather than 404s. If discovery is blocked (auth gate), it falls back to clearly-named
placeholder tokens and still emits a usable matrix.

Usage
-----
  gen-endpoint-matrix.py --repo <amp-analyzer-root> --out /tmp/endpoint-matrix.tsv
  gen-endpoint-matrix.py --repo <root> --out - --discover \
        --base-url https://amp-analyzer.amp-delivery.staging.corp.mongodb.com \
        --cookie-file ~/staging-cookie.txt
  gen-endpoint-matrix.py --repo <root> --save-vars /tmp/vars.json   # just discovery

Exit codes: 0 ok, 2 no controllers found.
"""

import argparse
import json
import os
import re
import ssl
import subprocess
import sys
import urllib.request

# --------------------------------------------------------------------------------------
# Comment stripping — Javadoc is full of {@code @GetMapping("/x")} examples that would
# otherwise be parsed as real endpoints. Preserve newlines so line numbers stay sane.
# --------------------------------------------------------------------------------------


def strip_comments(text):
    out = []
    i, n = 0, len(text)
    state = None  # None | 'line' | 'block' | 'str' | 'chr'
    while i < n:
        c = text[i]
        two = text[i:i + 2]
        if state is None:
            if two == "//":
                state = "line"
                i += 2
                continue
            if two == "/*":
                state = "block"
                i += 2
                continue
            if c == '"':
                state = "str"
            elif c == "'":
                state = "chr"
            out.append(c)
            i += 1
        elif state == "line":
            if c == "\n":
                state = None
                out.append(c)
            else:
                out.append(" ")
            i += 1
        elif state == "block":
            if two == "*/":
                state = None
                i += 2
                continue
            out.append("\n" if c == "\n" else " ")
            i += 1
        elif state == "str":
            if c == "\\":
                out.append(c)
                if i + 1 < n:
                    out.append(text[i + 1])
                i += 2
                continue
            if c == '"':
                state = None
            out.append(c)
            i += 1
        elif state == "chr":
            if c == "\\":
                out.append(c)
                if i + 1 < n:
                    out.append(text[i + 1])
                i += 2
                continue
            if c == "'":
                state = None
            out.append(c)
            i += 1
    return "".join(out)


# --------------------------------------------------------------------------------------
# Annotation helpers
# --------------------------------------------------------------------------------------

ANNOT_RE = re.compile(r"@([A-Za-z_][A-Za-z0-9_.]*)")


def balanced_end(text, open_idx, op="(", cl=")"):
    """Return index just past the matching close, given text[open_idx] == op."""
    depth = 0
    i = open_idx
    n = len(text)
    while i < n:
        c = text[i]
        if c == '"':
            i += 1
            while i < n and text[i] != '"':
                if text[i] == "\\":
                    i += 1
                i += 1
            i += 1
            continue
        if c == op:
            depth += 1
        elif c == cl:
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return n


def find_annotations(text, names):
    """Yield (name, start, end, args_str_or_None) for annotations in `names`."""
    for m in ANNOT_RE.finditer(text):
        name = m.group(1).split(".")[-1]
        if name not in names:
            continue
        j = m.end()
        k = j
        while k < len(text) and text[k] in " \t\r\n":
            k += 1
        args = None
        end = j
        if k < len(text) and text[k] == "(":
            end = balanced_end(text, k)
            args = text[k + 1:end - 1]
        yield name, j, end, args


STR_RE = re.compile(r'"((?:[^"\\]|\\.)*)"')


def strings_in(s):
    return [m.group(1) for m in STR_RE.finditer(s or "")]


def attr_value(args, attr):
    """Extract a named attribute's string literal(s) from an annotation arg body."""
    if not args:
        return None
    m = re.search(r"(?:^|[,(\s])(?:value|name|path)\s*=\s*", args)
    # fall back to positional for path/mapping style annotations
    if attr in ("value", "path"):
        # try explicit attr first
        m2 = re.search(r"(?:^|[,(\s])%s\s*=\s*" % attr, args)
        if m2:
            tail = args[m2.end():]
            ss = strings_in(tail[:tail.find(")") if ")" in tail else len(tail)])
            return ss[0] if ss else None
        # positional: first string literal
        ss = strings_in(args)
        return ss[0] if ss else None
    if m:
        tail = args[m.end():]
        ss = strings_in(tail)
        return ss[0] if ss else None
    return None


def attr_raw(args, attr):
    """Return the raw (unquoted) RHS token of `attr = <token>`."""
    if not args:
        return None
    m = re.search(r"(?:^|[,(\s])%s\s*=\s*([^,)]+)" % attr, args)
    if not m:
        return None
    return m.group(1).strip().strip('"')


# --------------------------------------------------------------------------------------
# Method-signature parsing
# --------------------------------------------------------------------------------------

def parse_params(sig):
    """
    Given the text from just after the mapping annotation(s) up to the method body, return
    the raw parameter-list string, plus whether the method carries a @RequestBody.

    Scans char-by-char so annotations (which contain their own parens) don't confuse the
    detection of the method's own parameter list.
    """
    i, n = 0, len(sig)
    params = ""
    while i < n:
        c = sig[i]
        if c == "@":
            m = ANNOT_RE.match(sig, i)
            if not m:
                i += 1
                continue
            j = m.end()
            while j < n and sig[j] in " \t\r\n":
                j += 1
            if j < n and sig[j] == "(":
                j = balanced_end(sig, j)
            i = j
            continue
        if c == "(":
            end = balanced_end(sig, i)
            params = sig[i + 1:end - 1]
            break
        if c in "{};":
            break
        i += 1
    return params


def split_top_commas(s):
    parts, depth, buf = [], 0, []
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        if c == '"':
            buf.append(c)
            i += 1
            while i < n and s[i] != '"':
                if s[i] == "\\":
                    buf.append(s[i])
                    i += 1
                if i < n:
                    buf.append(s[i])
                    i += 1
            if i < n:
                buf.append(s[i])
            i += 1
            continue
        if c in "(<[":
            depth += 1
        elif c in ")>]":
            depth -= 1
        if c == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
            i += 1
            continue
        buf.append(c)
        i += 1
    if "".join(buf).strip():
        parts.append("".join(buf))
    return parts


IDENT_RE = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*")


def parse_param(raw):
    """
    Parse one Java parameter into a dict:
      {kind: 'query'|'path'|'body'|'other', name, jtype, default, required, flag}
    """
    info = {"kind": "other", "name": None, "jtype": None,
            "default": None, "required": True, "flag": False}
    an = {}
    for name, _s, _e, args in find_annotations(raw, {"RequestParam", "PathVariable", "RequestBody"}):
        an[name] = args
    # strip annotations to get the bare declaration
    bare = raw
    for name, s, e, _a in sorted(
            find_annotations(raw, {"RequestParam", "PathVariable", "RequestBody"}),
            key=lambda t: -t[1]):
        full = ""
        # cut annotation incl. its parens
        m = ANNOT_RE.match(raw, s - 1)
        end = e
        bare = bare[:s - 1] + " " + bare[end:]
    decl = re.sub(r"\s+", " ", bare).strip()
    idents = IDENT_RE.findall(decl)
    var = idents[-1] if idents else None
    jtype = decl[:decl.rfind(var)].strip() if var else decl
    jtype = re.sub(r"\bfinal\b", "", jtype).strip()
    info["jtype"] = jtype
    info["name"] = var

    if "RequestBody" in an:
        info["kind"] = "body"
        return info
    if "PathVariable" in an:
        args = an["PathVariable"]
        nm = attr_value(args, "value") or attr_value(args, "name") if args else None
        info["kind"] = "path"
        info["name"] = nm or var
        return info
    if "RequestParam" in an:
        args = an["RequestParam"] or ""
        info["kind"] = "query"
        nm = None
        m = re.search(r"(?:value|name)\s*=\s*\"([^\"]*)\"", args)
        if m:
            nm = m.group(1)
        else:
            ss = strings_in(args)
            if ss:
                nm = ss[0]
        info["name"] = nm or var
        d = attr_raw(args, "defaultValue")
        if d is not None:
            info["default"] = d
        req = attr_raw(args, "required")
        if req is not None:
            info["required"] = (req.lower() != "false")
        info["flag"] = jtype in ("boolean", "Boolean")
        return info
    return info


# --------------------------------------------------------------------------------------
# Controller parsing
# --------------------------------------------------------------------------------------

MAPPINGS = {
    "GetMapping": "GET",
    "PostMapping": "POST",
    "PutMapping": "PUT",
    "DeleteMapping": "DELETE",
    "PatchMapping": "PATCH",
}

CLASS_RE = re.compile(r"\b(?:public\s+|final\s+|abstract\s+)*class\s+\w+")


def find_class_bases(text):
    """Return sorted list of (position, [base_paths]) for class-level @RequestMapping."""
    out = []
    for name, _s, end, args in find_annotations(text, {"RequestMapping"}):
        tail = text[end:end + 120]
        if CLASS_RE.match(tail.strip()):
            paths = strings_in(args) if args else []
            if not paths:
                paths = [""]
            out.append((end, paths))
    out.sort()
    return out


def parse_controller(path):
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        raw = fh.read()
    text = strip_comments(raw)
    cls = os.path.basename(path)
    bases = find_class_bases(text)
    base_positions = [p for p, _ in bases]

    def base_for(pos):
        chosen = [""]
        for p, paths in bases:
            if p <= pos:
                chosen = paths
            else:
                break
        return chosen

    endpoints = []
    for name, s, end, args in find_annotations(
            text, set(MAPPINGS) | {"RequestMapping"}):
        # class-level @RequestMapping already captured; skip it
        tail = text[end:end + 120].strip()
        if name == "RequestMapping" and CLASS_RE.match(tail):
            continue
        if name == "RequestMapping":
            meth = attr_raw(args, "method")
            verb = meth.split(".")[-1].replace("RequestMethod", "").strip() if meth else None
            verb = (verb or "ANY").upper()
        else:
            verb = MAPPINGS[name]

        paths = strings_in(args) if args else []
        if not paths:
            paths = [""]

        sig = text[end:]
        # stop at the method body brace / abstract ;
        stop = len(sig)
        for ch in ("{", ";"):
            idx = sig.find(ch)
            if idx >= 0:
                stop = min(stop, idx)
        sig = sig[:stop]
        # strip any trailing annotations (@PreAuthorize etc.) before params
        params_raw = parse_params(sig)
        params = [parse_param(p) for p in split_top_commas(params_raw) if p.strip()]

        stream = bool(re.search(r"TEXT_EVENT_STREAM_VALUE", args or ""))
        has_body = any(p["kind"] == "body" for p in params)
        for b in base_for(s):
            for mp in paths:
                full = join_path(b, mp)
                endpoints.append({
                    "method": verb,
                    "path": full,
                    "controller": cls,
                    "stream": stream,
                    "has_body": has_body,
                    "params": params,
                })
    return endpoints


def join_path(a, b):
    a = (a or "").rstrip("/")
    b = (b or "").strip()
    if not b:
        return a or "/"
    if not b.startswith("/"):
        b = "/" + b
    return (a + b) or "/"


def collect(repo):
    root = os.path.join(repo, "unified-web", "src", "main")
    files = []
    for dirpath, _dirs, names in os.walk(root):
        for f in names:
            if f.endswith("Controller.java"):
                files.append(os.path.join(dirpath, f))
    files.sort()
    eps = []
    for f in files:
        eps.extend(parse_controller(f))
    return eps


# --------------------------------------------------------------------------------------
# Variant policy — what values to try for each @RequestParam, by name.
# --------------------------------------------------------------------------------------

def policy(name, default, jtype, ctx):
    """Return list of (label, value). value None => parameter omitted entirely."""
    n = (name or "").lower()
    t = (jtype or "").lower()

    fixes = {
        "scope": ["full", "subflow", "subtree", "direct"],
        "sort": ["TIME_DESC", "NAME_ASC"],
        "limit": ["25", "200"],
        "after": [],                      # opaque cursor: baseline-only
        "q": [ctx.get("q") or "PROC", ctx.get("table") or "TABLE"],
        "search": [ctx.get("table") or "TABLE"],
        "cf": ["name:" + (ctx.get("analysis") or "x")],
        "version": ["1"],
        "source": ["artifact", "entity"],
        "mode": ["static", "dynamic"],
        "format": ["json", "csv"],
        "level": ["INFO", "ERROR"],
        "types": ["TABLE", "TABLE,VIEW"],
        "type": ["TABLE"],
        "algorithm": ["logarithmic"],
        "dialect": ["ORACLE"],
        "op": ["find", "aggregate"],
        "collection": [ctx.get("collection") or "test"],
    }
    if n in fixes:
        vals = fixes[n]
        # keep the code default first when we have one and it isn't already listed
        out = []
        if default is not None:
            out.append(("default", default))
        for v in vals:
            if default is not None and v == default:
                continue
            out.append((v, v))
        return out or [("default", default)]

    flags = {"resume", "members", "dryrun", "full", "force", "refresh", "enabled", "stream"}
    if n in flags or t in ("boolean", "bool"):
        if default is not None:
            alt = "false" if str(default).lower() == "true" else "true"
            return [("default", default), (alt, alt)]
        return [("true", "true"), ("false", "false")]

    if n in ("procid", "directprocid", "nodeid", "nodeids"):
        v = ctx.get("procId") or "1"
        return [("default", v)]

    if n in ("tablename", "table"):
        v = ctx.get("table") or "TABLE"
        return [("default", v)]

    if n in ("endpoint", "endpointname"):
        v = ctx.get("endpoint") or "1"
        return [("default", v)]

    if n in ("filename",):
        return [("default", ctx.get("fileName") or "corrections.json")]

    if n in ("sessionid", "jobid", "crossrefid", "key", "idx"):
        return [("default", "1")]

    if n in ("username",):
        return [("default", ctx.get("username") or "SCOTT")]
    if n in ("schema",):
        return [("default", ctx.get("schema") or "SCOTT")]
    if n in ("objectname", "objectinput", "packagename"):
        return [("default", ctx.get("objectName") or "PKG_1")]
    if n in ("project", "projectpath", "env", "environment", "connname"):
        return [("default", ctx.get(n) or "")]

    if t in ("int", "integer", "long", "long", "short"):
        out = []
        if default is not None:
            out.append(("default", default))
        out += [("1", "1"), ("100", "100")]
        return out

    if default is not None:
        # numeric-looking string default (weights) — try default + a perturbed value
        try:
            f = float(default)
            alt = str(int(f) * 2 if f >= 1 else 1)
            return [("default", default), (alt, alt)] if alt != default else [("default", default)]
        except ValueError:
            return [("default", default)]

    # unknown free string: if required we must send *something*
    return [("default", None)]


def is_required_no_default(p):
    return p["kind"] == "query" and p["required"] and p["default"] is None


def required_fallback(name, jtype):
    n = (name or "").lower()
    t = (jtype or "").lower()
    if n in ("path", "file", "configfile"):
        return "pom.xml"
    if n in ("schema",):
        return "SCOTT"
    if n in ("objectname", "objectinput", "packagename"):
        return "PKG_1"
    if n in ("key",):
        return "1"
    if n in ("class", "collection"):
        return "1"
    if n in ("table", "tablename"):
        return "TABLE"
    if t in ("int", "integer", "long", "short"):
        return "1"
    return "1"


def build_rows(ep, ctx):
    """Baseline row + 1-factor-at-a-time rows for one endpoint."""
    qparams = [p for p in ep["params"] if p["kind"] == "query"]
    base_query = []
    for p in qparams:
        if p["default"] is not None:
            base_query.append((p["name"], p["default"]))
        elif p["required"]:
            base_query.append((p["name"], required_fallback(p["name"], p["jtype"])))
        # optional, no default -> omit at baseline
    base_q = encode_query(base_query)

    mutating = ep["method"] not in ("GET", "HEAD")
    common = (ep["method"], ep["path"], ep["stream"], mutating, ep["controller"])
    rows = [(common[0], common[1], base_q, "baseline", int(ep["stream"]),
             int(mutating), ep["controller"])]

    for p in qparams:
        variants = policy(p["name"], p["default"], p["jtype"], ctx)
        for label, val in variants:
            if p["default"] is not None and label == "default":
                continue  # same as baseline
            if val is None and p["default"] is None:
                continue
            q = [(k, v) for (k, v) in base_query if k != p["name"]]
            if val is not None:
                q.append((p["name"], val))
            rows.append((common[0], common[1], encode_query(q),
                         "%s=%s" % (p["name"], val if val is not None else "<omitted>"),
                         int(ep["stream"]), int(mutating), ep["controller"]))
    return rows


def encode_query(pairs):
    from urllib.parse import quote
    return "&".join("%s=%s" % (quote(str(k), safe=""), quote(str(v), safe=""))
                    for k, v in pairs)


# --------------------------------------------------------------------------------------
# Runtime discovery — fetch real ids so {path} templates hit real data.
# --------------------------------------------------------------------------------------

def http_get(base_url, path, cookie, timeout=30):
    url = base_url.rstrip("/") + path
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")
    if cookie:
        req.add_header("Cookie", cookie)
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001 - discovery is best-effort
        return 0, "ERROR: %s" % e


def discover(base_url, cookie, log=None):
    """Best-effort fetch of REAL ids. Never fatal: returns whatever it can."""
    if log is None:
        def log(m):
            sys.stderr.write(str(m) + "\n")
    ctx = {}
    status = {}

    code, body = http_get(base_url, "/api/parser/analyses", cookie)
    status["/api/parser/analyses"] = code
    names = []
    if code == 200:
        try:
            data = json.loads(body)
            if isinstance(data, dict) and "rows" in data:
                data = data["rows"]
            for a in data or []:
                nm = a.get("name") if isinstance(a, dict) else None
                if nm:
                    names.append(nm)
        except Exception:
            pass
    if names:
        ctx["analysis"] = names[0]
        ctx["analyses"] = names

    for ep, key in (("/api/jar/jars", "jarIds"), ("/api/war/wars", "warIds")):
        code, body = http_get(base_url, ep, cookie)
        status[ep] = code
        ids = []
        if code == 200:
            try:
                data = json.loads(body)
                for j in data or []:
                    jid = j.get("id") if isinstance(j, dict) else None
                    if jid:
                        ids.append(jid)
            except Exception:
                pass
        if ids:
            ctx[key] = ids
    if ctx.get("jarIds"):
        ctx["jarId"] = ctx["jarIds"][0]
    if ctx.get("warIds"):
        ctx["warId"] = ctx["warIds"][0]

    # analysis-derived ids: a node/proc id, a table name, an endpoint name
    if ctx.get("analysis"):
        an = ctx["analysis"]
        code, body = http_get(base_url, "/api/parser/analyses/%s/index" % an, cookie)
        status["index"] = code
        if code == 200:
            try:
                d = json.loads(body)
                nodes = d.get("nodes") or []
                for nd in nodes:
                    if not ctx.get("procId") and nd.get("id"):
                        ctx["procId"] = nd["id"]
                    if not ctx.get("fileName") and nd.get("file"):
                        ctx["fileName"] = nd["file"]
                    if not ctx.get("nodeId") and nd.get("nodeId"):
                        ctx["nodeId"] = nd["nodeId"]
                    if ctx.get("procId") and ctx.get("fileName"):
                        break
            except Exception:
                pass
        code, body = http_get(base_url, "/api/parser/analyses/%s/tables/list" % an, cookie)
        status["tables/list"] = code
        if code == 200:
            try:
                d = json.loads(body)
                rows = d.get("rows") if isinstance(d, dict) else d
                for t in rows or []:
                    tn = t.get("tableName") or t.get("name") if isinstance(t, dict) else t
                    if tn:
                        ctx["table"] = tn
                        break
            except Exception:
                pass

    log("discovery status: " + ", ".join("%s=%s" % (k, v) for k, v in status.items()))
    return ctx


# --------------------------------------------------------------------------------------
# Placeholder fallback when discovery produced nothing
# --------------------------------------------------------------------------------------

def apply_placeholders(path, ctx):
    """Substitute {var} tokens in a path template using discovered ctx, else leave token."""
    def repl(m):
        var = m.group(1)
        key = {
            "name": "analysis", "id": "id", "jarId": "jarId", "warId": "warId",
            "procId": "procId", "directProcId": "procId", "nodeId": "nodeId",
            "tableName": "table", "table": "table", "idx": "idx", "file": "fileName",
            "endpoint": "endpoint", "endpointName": "endpoint", "fileName": "fileName",
            "sessionId": "sessionId", "jobId": "jobId", "crossRefId": "crossRefId",
            "key": "key", "username": "username", "schema": "schema",
            "packageName": "packageName", "objectName": "objectName",
            "objectInput": "objectName", "collection": "collection",
        }.get(var, var)
        if key == "id":
            # {id} means the war id under /api/war, the jar id otherwise
            key = "warId" if path.startswith("/api/war") else "jarId"
        val = ctx.get(key)
        if val is None:
            return "{%s}" % var  # leave the token: sweep will report it as unresolved
        return str(val)
    return re.sub(r"\{([A-Za-z0-9_]+)\}", repl, path)


# --------------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo", default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    ap.add_argument("--out", default="-", help="matrix TSV path ('-' = stdout)")
    ap.add_argument("--discover", action="store_true",
                    help="fetch real ids from the running app (needs --base-url)")
    ap.add_argument("--base-url", default=os.environ.get(
        "AMP_BASE_URL", "https://amp-analyzer.amp-delivery.staging.corp.mongodb.com"))
    ap.add_argument("--cookie-file", default=os.path.expanduser("~/staging-cookie.txt"))
    ap.add_argument("--save-vars", help="write discovered vars JSON here and exit")
    ap.add_argument("--no-substitute", action="store_true",
                    help="keep {tokens} in paths instead of substituting discovered ids")
    args = ap.parse_args()

    eps = collect(args.repo)
    if not eps:
        sys.stderr.write("no controllers found under %s\n" % args.repo)
        return 2

    ctx = {}
    cookie = None
    if args.cookie_file and os.path.exists(args.cookie_file):
        with open(args.cookie_file) as fh:
            cookie = fh.read().strip()
    elif os.environ.get("AMP_COOKIE"):
        cookie = os.environ["AMP_COOKIE"]

    if args.discover or args.save_vars:
        ctx = discover(args.base_url, cookie)
        # defaults so templates are always materialisable
        ctx.setdefault("idx", "0")
        ctx.setdefault("endpoint", "1")
        ctx.setdefault("sessionId", "1")
        ctx.setdefault("jobId", "1")
        ctx.setdefault("crossRefId", "1")
        ctx.setdefault("key", "1")
        ctx.setdefault("username", "SCOTT")
        ctx.setdefault("schema", "SCOTT")
        ctx.setdefault("packageName", "PKG_1")
        ctx.setdefault("objectName", "PKG_1")
        ctx.setdefault("collection", "1")
        if args.save_vars:
            with open(args.save_vars, "w") as fh:
                json.dump(ctx, fh, indent=2)
            print("wrote %s" % args.save_vars)
            return 0

    rows = []
    for ep in eps:
        path = ep["path"]
        if not args.no_substitute:
            path = apply_placeholders(path, ctx)
        rows.extend(build_rows(dict(ep, path=path), ctx))

    # '|' delimiter (not \t): a TAB is IFS-whitespace in bash, so `read` collapses the empty
    # fields (e.g. an endpoint with no query string) and every column after it shifts by one.
    # '|' is non-whitespace, so empty fields survive round-trips; none of our field values
    # contain a literal '|' (queries are URL-encoded, so a '|' there becomes %7C).
    header = ["method", "path_template", "query", "variant", "stream",
              "mutating", "controller"]
    lines = ["|".join(header)]
    for r in rows:
        lines.append("|".join(str(x) for x in r))
    payload = "\n".join(lines) + "\n"

    if args.out == "-":
        sys.stdout.write(payload)
    else:
        with open(args.out, "w") as fh:
            fh.write(payload)

    # counts to stderr so stdout stays a clean matrix
    n_ep = len(set((e["method"], e["path"]) for e in eps))
    sys.stderr.write("controllers=%d endpoints=%d combinations=%d%s\n" % (
        len(set(e["controller"] for e in eps)), n_ep, len(rows),
        "" if args.no_substitute else ", ids=%s" % json.dumps(ctx, default=str)[:200]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
