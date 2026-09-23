# PLAN-10 — The workflows that drive and verify this build

PLAN-00's index listed this file as **done** while it did not exist. The skills it describes were
built and do work; the document did not. This is that document, written from the skills as they
actually are on disk.

Each is a Claude Code skill under `.claude/skills/`, invoked as a slash command, so a check that
used to need a paragraph of instructions needs a word.

## 1. `/storage-check` — is anything still writing locally?

`.claude/skills/storage-check/` — `SKILL.md`, `scripts/scan.py`, `OPEN-ITEMS.md`

The constraint it exists to defend: **nothing may be written to the local filesystem**, because the
same code has to run against S3 and MongoDB where no such path exists. The migration moved 783
direct I/O operations down to the survivors below, and this reports what is left, by module and by
reason.

```
$ python3 .claude/skills/storage-check/scripts/scan.py

module                   ops          category      ops
unified-web              156          temps         109
jar-analyzer-core         52          config         57
plsql-parser              11          logging        21
plsql-config               3          outside        13
flow-poc                   2          credentials    10
TOTAL                    224          scratch         7
                                      bootstrap       4
                                      subprocess      3
```

**224 operations, every one classified.** The point is not the number, it is that no category is
"unexplained". The eight reasons are the only legitimate ones:

- **temps** — a multipart upload arrives as a local temp; `JarFile`/CFR need a *seekable* file. The
  stored artifact is always reached through `store.materialize(key)`, never an assumed path.
- **config** — read before any store exists. You need config to know the bucket, so it cannot live
  in the bucket.
- **credentials** — plaintext production DB passwords that must never reach shared storage.
  `CredentialGuardStore` throws on these leaf names.
- **scratch** — process-local, in `java.io.tmpdir`, needing append and seek, which the store
  contract deliberately does not offer. Never touches the data tree.
- **logging**, **outside**, **bootstrap**, **subprocess** — operational, the operator's own
  filesystem, the one `Path.of` that builds the store root, and `ProcessBuilder`'s working
  directory.

A new unclassified op is a finding. The permanent enforcement is `DirectFileIoGateTest`, which holds
a per-class budget that may only ever ratchet **down**; this skill is the human-readable view of the
same data.

## 2. `/s3-mode` — prove nothing lands on disk

`.claude/skills/s3-mode/` — `SKILL.md`, `scripts/no-local-writes.sh`

```
usage: no-local-writes.sh {snapshot <file> | verify <before> <after>}
```

Snapshot the data tree, run a full analysis in `mode=s3`, verify no new local artifact appeared. The
harness is self-tested: it was checked to *fail* when a file is planted, because a verifier that
cannot fail proves nothing.

**Status: blocked.** Requires live AWS credentials; the ones supplied in session were STS and have
expired. `mode=s3` was previously verified end-to-end with zero local artifact writes, but
`mode=s3` **combined with** `repository=mongo` has never been exercised.

## 3. `/db-mode` — the field census behind the schema

`.claude/skills/db-mode/` — `SKILL.md`, `scripts/field-scan.py`

Walks every JSON under `data/plsql-parse`, `data/jar` and `data/plsql`, recording each field path and
the **set of types actually observed** for it, per artifact. Writes `/tmp/fieldmap.json`.

This is what produced PLAN-11, and it answers the one question that cannot be answered by reading
code: **which fields are sometimes a scalar and sometimes an array.** Jackson accepts both without
complaint, so a schema assuming one is wrong only for some inputs, and the failure is a missing row
rather than an exception. Six such paths exist in real data; two of them
(`endpoints`, `modules`) are name collisions where the same identifier means a *count* in one
artifact and a *collection* in another — which is why the rule is **map by (artifact, path), never
by field name**.

Re-run it whenever new analyses land.

## 4. What these do not cover

- No workflow drives PLAN-09's cutover (Increments G and H). That is deliberate: G and H **delete
  the legacy copy**, and nothing consumes the entity layer yet, so an automated path to an
  irreversible step is a liability rather than a convenience.
- `/s3-mode` cannot run without credentials, so the s3+mongo combination stays unverified.
