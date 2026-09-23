/**
 * Generates the API fixtures the scale tests run against.
 *
 * ## Two sizes, and why the large one is generated rather than committed
 *
 * "small" mirrors today's shape and row counts. "large" is the size the product is actually being
 * aimed at — the stated targets are 8,000 procedures / 1,000,000 DB operations and a 100M-line Java
 * codebase, against a current corpus of 356 procedures and 15,824 statements.
 *
 * The large fixture is built by this script instead of being checked in, because a committed 50 MB
 * JSON is a file nobody reads, nobody updates, and everybody clones forever. Generating it also
 * makes the size a parameter, so a test can ask for the volume it needs.
 *
 * Usage:  node tests/fixtures/generate.mjs [--rows 50000] [--out dir]
 */
import { mkdir, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const HERE = path.dirname(fileURLToPath(import.meta.url));

const OPERATIONS = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'MERGE'];
const ACCESS_KINDS = ['READ', 'WRITE', 'JOIN_READ', 'AGGREGATE_READ', 'SUBQUERY_READ'];

/**
 * Rows shaped like the real statements index.
 *
 * <p>Deliberately not uniform. Real data is skewed — a few tables carry most of the traffic, some
 * statements have no columns at all — and a fixture of identical rows would let an O(n²) render or
 * a broken group-by look fine, because every row costs the same and every group has one member.
 */
function statements(count) {
    const rows = [];
    const tables = Math.max(1, Math.floor(count / 140));      // ~140 statements per table, as measured
    for (let i = 0; i < count; i++) {
        // Zipf-ish: table 0 gets far more traffic than table N, as in the real corpus.
        const t = Math.min(tables - 1, Math.floor(Math.abs(Math.sin(i * 12.9898)) * tables));
        const op = OPERATIONS[i % OPERATIONS.length];
        const hasColumns = i % 3 !== 0;                        // a third have none, as measured
        rows.push({
            nodeId: `ERP$PROC_${i % Math.max(1, Math.floor(count / 60))}`,
            nodeName: `PROC_${i % Math.max(1, Math.floor(count / 60))}`,
            schema: 'ERP',
            tableName: `ERP_TABLE_${t}`,
            operation: op,
            line: (i % 900) + 1,
            accessKind: ACCESS_KINDS[i % ACCESS_KINDS.length],
            columnsParsed: hasColumns,
            columns: hasColumns ? [`COL_${i % 40}`, `COL_${(i + 7) % 40}`] : [],
            columnDetail: hasColumns
                ? [{ name: `COL_${i % 40}`, usage: op === 'SELECT' ? 'SELECT' : 'SET' }]
                : [],
            predicates: i % 4 === 0 ? [{ column: `COL_${i % 40}`, operator: '=', value: ':p' }] : [],
            joins: i % 5 === 0 ? [{ type: 'INNER', right: `ERP_TABLE_${(t + 1) % tables}` }] : [],
            sqlText: `SELECT COL_${i % 40} FROM ERP_TABLE_${t} WHERE COL_1 = :p /* ${i} */`,
        });
    }
    return rows;
}

function endpoints(count) {
    const rows = [];
    for (let i = 0; i < count; i++) {
        rows.push({
            controllerClass: `com.example.Controller${i % Math.max(1, Math.floor(count / 12))}`,
            methodName: `handler${i}`,
            httpMethod: ['GET', 'POST', 'PUT', 'DELETE'][i % 4],
            fullPath: `/api/resource/${i}`,
            totalDbOperations: i % 37,
            totalLoc: 20 + (i % 400),
            totalMethods: 1 + (i % 25),
        });
    }
    return rows;
}

const args = process.argv.slice(2);
const rows = Number(args[args.indexOf('--rows') + 1]) || 50000;
const outDir = args.includes('--out') ? args[args.indexOf('--out') + 1] : HERE;

await mkdir(outDir, { recursive: true });

const small = { statements: statements(200), endpoints: endpoints(20) };
const large = { statements: statements(rows), endpoints: endpoints(Math.max(300, Math.floor(rows / 100))) };

await writeFile(path.join(outDir, 'small.json'), JSON.stringify(small));
await writeFile(path.join(outDir, 'large.json'), JSON.stringify(large));

const mb = (o) => (JSON.stringify(o).length / 1e6).toFixed(1);
console.log(`  small.json  ${small.statements.length} statements, ${small.endpoints.length} endpoints  (${mb(small)} MB)`);
console.log(`  large.json  ${large.statements.length} statements, ${large.endpoints.length} endpoints  (${mb(large)} MB)`);
