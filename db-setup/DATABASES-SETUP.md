# Analyzer Demo Databases - Setup

One-command bring-up of the **5 demo databases** used by the analyzer, each
running in Docker and seeded with a representative **ERP** schema (tables,
procedures/functions, and minimal sample data) so that analysis has a real
multi-level call graph to traverse.

| DB | Container | Entry procedure |
|----|-----------|-----------------|
| Oracle Free | `oracle-free` | `ERP_USP_RUN_CYCLE` |
| MySQL 8 | `mysql8` | `ERP_USP_RUN_CYCLE` |
| Db2 | `db2` | `ERP_USP_RUN_CYCLE` |
| SQL Server 2019 | `sqlserver2019` | `USP_RUNMRPCYCLE` |
| PostgreSQL 16 | `postgres16` | `usp_run_cycle` |

---

## 1. Prerequisites

- **Docker** installed and able to pull images:
  - macOS / Windows: **Docker Desktop**
  - Linux: the `docker` engine (the script will try `sudo systemctl start docker`)
- On macOS, if `docker` is not on your `PATH`, the script automatically adds
  `/Applications/Docker.app/Contents/Resources/bin`.
- Network access to pull: `gvenzl/oracle-free`, `mysql:8`,
  `icr.io/db2_community/db2`, `mcr.microsoft.com/mssql/server:2019-latest`,
  `postgres:16`.
- Free host ports: **1521, 3307, 50000, 1433, 5432**.
- Db2 requires the container to run **`--privileged`** (the script sets this).
- First-run image pulls plus Oracle/Db2 initialisation can take **several
  minutes**; the scripts poll with generous timeouts.

---

## 2. How to run

### macOS / Linux
```bash
cd db-setup
./setup-databases.sh              # bring up ALL 5 databases
./setup-databases.sh postgres     # or just one (or several)
./setup-databases.sh oracle db2   # space-separated list
```

### Windows
```bat
cd db-setup
setup-databases.bat               :: bring up ALL 5 databases
setup-databases.bat postgres      :: or just one (or several)
setup-databases.bat oracle db2
```

What the scripts do, per database:
1. Ensure Docker is installed and the daemon is running (starting it if needed).
2. If the container already exists, `docker start` it; otherwise `docker run -d`
   it with the exact name/port/credentials below.
3. Wait until the DB accepts connections (using its native check -
   `pg_isready`, `mysqladmin ping`, `sqlplus`, `sqlcmd`, `db2 connect` - with a
   TCP port fallback). Oracle and Db2 get longer timeouts.
4. Seed it **idempotently** from `db-setup/seed/<dialect>.sql`.
5. Print a summary table.

Everything is **safe to re-run** - containers are reused and seeds reset their
own data.

---

## 3. Connection details (summary table)

| Container | Image | Host | Port | DB / Service | User | Password | Entry procedure |
|-----------|-------|------|------|--------------|------|----------|-----------------|
| `oracle-free` | `gvenzl/oracle-free` | localhost | 1521 | `FREEPDB1` (schema `ANALYZER`) | `analyzer` | `Password123!` | `ERP_USP_RUN_CYCLE` |
| `mysql8` | `mysql:8` | localhost | 3307 -> 3306 | `analyzerdb` | `root` | `Password123!` | `ERP_USP_RUN_CYCLE` |
| `db2` | `icr.io/db2_community/db2` | localhost | 50000 | `anlzdb` | `db2inst1` | `Password123` *(no `!`)* | `ERP_USP_RUN_CYCLE` |
| `sqlserver2019` | `mcr.microsoft.com/mssql/server:2019-latest` | localhost | 1433 | `analyzer-db` (schema `erp`) | `sa` | `Password123!` | `USP_RUNMRPCYCLE` |
| `postgres16` | `postgres:16` | localhost | 5432 | `analyzerdb` (schema `erp`) | `postgres` | `Password123!` | `usp_run_cycle` |

> Note the Db2 password has **no trailing `!`** - this matches the app config.

---

## 4. Entry procedures & call chains

Each seed builds a small but genuine call chain rooted at the entry procedure so
the analyzer produces a meaningful call graph.

| DB | Entry procedure | Calls (children) |
|----|-----------------|------------------|
| Oracle | `ERP_USP_RUN_CYCLE` | `ERP_USP_WRITE_AUDIT_TRAIL`, `ERP_USP_PROCESS_ORDER` -> `ERP_FN_ORDER_TOTAL` |
| MySQL | `ERP_USP_RUN_CYCLE` | `ERP_USP_AUDIT_LOG`, `ERP_USP_PROCESS_ALL_ORDERS` -> `ERP_USP_PROCESS_ORDER` -> `ERP_FN_ORDER_TOTAL` -> `ERP_FN_LINE_AMOUNT` |
| Db2 | `ERP_USP_RUN_CYCLE` | `ERP_USP_AUDIT`, `ERP_USP_PROCESS_ORDER` -> `ERP_FN_ORDER_SUBTOTAL` -> `ERP_FN_LINE_TOTAL` |
| SQL Server | `usp_RunMrpCycle` | `usp_WriteAuditTrail`, `usp_ProcessSalesOrder` -> `fn_order_total`, `fn_UnitCost` |
| PostgreSQL | `usp_run_cycle` | `usp_process_order` -> `fn_order_total`; `usp_post_gl` -> `fn_order_total` |

Each chain does INSERT/UPDATE/SELECT across a few ERP tables, `COMMIT`s on
success, and `ROLLBACK`s on the exception path.

> **Assumptions / notes.** These seeds are **representative**, not byte-for-byte
> copies of the original analyzed sources. The real sources
> (`data/plsql-parse/*/sources/*.sql`) contain very large cycle procedures that
> touch ~30 tables. To keep bring-up fast and reliable, the seeds preserve the
> **exact entry-procedure names**, the **table/column naming conventions**, and
> the **dialect-specific structure** (cursors, handlers, TRY/CATCH, MERGE, etc.)
> of each source, on a reduced schema. Anything reconstructed is flagged with a
> `NOTE:` comment inside the per-dialect `.sql` file. To analyze the full
> original procedures, load the sources under `data/plsql-parse/` directly.

---

## 5. Mapping to the app's demo projects

The app ships five demo projects; each maps to one container above:

| Demo project | Database | Entry procedure |
|--------------|----------|-----------------|
| **OracleDemo** | `oracle-free` / `FREEPDB1` (schema `ANALYZER`) | `ERP_USP_RUN_CYCLE` |
| **MysqlDemo** | `mysql8` / `analyzerdb` | `ERP_USP_RUN_CYCLE` |
| **Db2Demo** | `db2` / `anlzdb` | `ERP_USP_RUN_CYCLE` |
| **SqlServerDemo** | `sqlserver2019` / `analyzer-db` (schema `erp`) | `USP_RUNMRPCYCLE` |
| **PostgresDemo** | `postgres16` / `analyzerdb` (schema `erp`) | `usp_run_cycle` |

After running the setup script, open the matching demo project in the analyzer
and run analysis from the listed entry procedure. The connection host/port/creds
are those in the summary table (section 3).

---

## 6. Files in this directory

```
db-setup/
├── setup-databases.sh      # macOS/Linux bring-up + seed
├── setup-databases.bat     # Windows bring-up + seed
├── DATABASES-SETUP.md      # this document
└── seed/
    ├── oracle.sql          # schema ANALYZER, entry ERP_USP_RUN_CYCLE
    ├── mysql.sql           # db analyzerdb, entry ERP_USP_RUN_CYCLE
    ├── db2.sql             # db anlzdb (terminator '@'), entry ERP_USP_RUN_CYCLE
    ├── sqlserver.sql       # db analyzer-db / schema erp, entry usp_RunMrpCycle
    └── postgres.sql        # db analyzerdb / schema erp, entry usp_run_cycle
```

---

## 7. Troubleshooting

- **Port already in use** - stop whatever owns the port, or remove the old
  container: `docker rm -f <name>`, then re-run.
- **Oracle / Db2 "not reachable" timeout** - first-run initialisation is slow;
  wait and re-run the script (it will reuse the container and just seed).
- **MySQL function-creation error** - the container is started with
  `--log-bin-trust-function-creators=1` and the script also sets the global at
  runtime; if you created the container by hand without it, run:
  `docker exec mysql8 mysql -uroot -pPassword123! -e "SET GLOBAL log_bin_trust_function_creators=1;"`
- **Db2 DROP errors on first run** - expected and harmless (objects don't exist
  yet); the CLP continues.
- **`sqlcmd` not found** - newer SQL Server images use
  `/opt/mssql-tools18/bin/sqlcmd` (needs `-C`); the scripts try both that and
  the older `/opt/mssql-tools/bin/sqlcmd`.
