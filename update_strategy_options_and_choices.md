This is a checklist for refactoring and building out the update strategy options and implementations.

High-level decisions captured

- Canonical current-state term: use "current" (df_current) as the canonical current-state artifact (base >> (updates) >> current).
- SQL source: use materialized views as the canonical “current” artifact (materialized, manual refresh).
- Materialization of current (parquet or SQL) will be produced in generate_data and is not part of timed ingest/compare runs.
- For this first update run, treat both source_current and destination as one-row-per-id snapshots (we're not yet worrying about latest-per-id deduping, but recognize there will be dup ids after compare and increments are run).
- Don't change existing file/table naming patterns — simply add "current" alongside "base" and "updates" following existing conventions.

Generate_data checklist

- Produce current for both source types (alongside base and updates which already are being created):
  - Parquet path: write a current file/dataset (matching existing patterns) by logically applying updates to base.
  - SQL path: create or replace the materialized view that serves as current.
- Validation step: after materialization, read back basic sanity checks (schema match, row counts) and record them in metadata.
- Record generate_data as metadata-only metrics rows (controller_name = "BFF-Controller" etc.) so they are available for traceability but excluded from benchmark aggregates.

Timing & metrics rules

- Do NOT include current creation/materialization time in the benchmark ingest/compare timers. Record a separate metadata metric row for materialization (start_ts, end_ts, duration, artifact type, seed, controller_name).
  - Do include the time it takes to "bring in" data from an external source, for example, SQL-to-WH staging.
- For each benchmark run, record at minimum: start_ts, end_ts, duration_s, rows_read, rows_written, rows_inserted, rows_updated, rows_deleted, source_type (parquet|sql), destination_type (delta|warehouse), seed, controller_name, is_cold_run/warm_run, and a flag marking generate_data rows as excluded_from_benchmark. (Keep metrics minimal, consistent and queryable.)
- Watch for platform-level optimization effects (caching/compile) and mark cold vs warm runs explicitly.

Ingest/update strategies (per strategy)

Full Refresh:
- Input: use current (materialized or parquet) as the source of truth.
- Behavior: overwrite destination with current (destination-level overwrite as per existing flow).
- Timers: start when either staging or reading of current begins.

Full Compare:
- Input: read source_current and read destination snapshot (both are 1-row-per-id for this phase).
- Compare logic (timestamp-driven):
  - id in source only → insert (append row with update_type = "insert").
  - id in dest only → delete/tombstone (append row with update_type = "delete").
  - id in both and source.ts > dest.ts → update (append row with update_type = "update").
  - id in both and dest.ts > source.ts → anomaly (log/count as error; none expected in our data).
- Output: append classified event rows to destination event-log/target (do not mutate existing rows).
- The current will have latest-per-id data by definition.  Use logic of latest-per-id for destination but know it won't have performance implications because this is simulating a base dataset at first update.
- Timers: start when reads begin and stop after writes complete; record class counts and anomaly counts.

Incremental:
- Behavior: read source updates and append them to destination (fastest/simple path per combo).
- Timers: start at update read; stop after write completes. Record rows read/written and duration.
- No intermediate persistence in Spark flows; use staging for warehouse when needed and drop/truncate staging at the end.

Source→Destination recommended mechanics (implementation-level choices, kept minimal)

- Parquet → Delta / Parquet → Warehouse:
  - Use the current parquet dataset produced in generate_data for full-refresh and full-compare flows; use updates parquet for incremental.
  - Read in Spark (partitioned) for compare and incremental; for full-refresh write current as the overwrite target.
- Azure SQL → Delta:
  - Use materialized view (current) in the database as the source; read via partitioned JDBC or predicate-slicing into Spark for compare or incremental.
  - For full-refresh read the materialized view and overwrite target.
- Azure SQL → Warehouse:
  - For this benchmark path use fast bulk ingest of current into a warehouse staging schema, perform the compare/join server-side in the warehouse, write events, then drop/truncate the staging table(s).
  - Use bulk-export/bulk-load patterns for incremental updates into the warehouse staging area, then CTAS/INSERT as appropriate, then cleanup.

Spark behavior and persistence rules

- Spark ops remain ephemeral by default. Avoid writing intermediate artifacts in Spark flows.
- Use df.persist()/unpersist only where performance requires it, and ensure unpersist is called in cleanup.
- Explicitly unpersist and drop temp views at the end of each flow to avoid cross-run contamination.

Cleanup & lifecycle

- SQL→WH staging: always DROP/TRUNCATE staging tables at flow end so staging can be reused.
- Materialized views / current artifacts: generate_data creates them; include TTL/cleanup guidance in generate_data (but do not automatically delete without an explicit retention policy).
- Notebook comments/markers: include clearly labeled blocks for [SETUP], [MATERIALIZE CURRENT] (generate_data), [COMPARE / WRITE] (timed), and [CLEANUP] (unpersist, drop staging, remove temp files).

Audit/logging and anomalies

- For full compare record counts for inserts, updates, deletes, and anomalies (dest.ts > source.ts). Persist these counts in the metrics table with timestamps.
- Include metadata rows for generate_data materializations (controller_name, event="generate_data", artifact=current/updates, start_ts, end_ts, duration, note=method) and mark them excluded_from_benchmark.
- Correlate with Capacity Workspace telemetry by including a run identifier and workspace id/displayName in the benchmark run metrics (so Fabric Capacity data can be joined externally). These fields are for metric correlation only — follow your existing privacy/visibility rules.

Implementation pragmatics to defer to coding time

- Do not modify file/table naming patterns now — when we implement, match existing patterns and add current in the same style as base & updates.
- Concrete partitioning, file size tuning, JDBC partitionColumn choices, and exact SQL used for server-side joins will be filled in during the implementation step.
- Permissions/identities: nothing new or changing.  Everything runs in a service principal account or by a user with full permissions.

Captured narrative

- Use "current" as canonical, materialize current in generate_data, do not include materialization in timed metrics, use SQL materialized views for SQL sources, use warehouse staging + server-side join for sql→wh, and keep spark flows ephemeral. Full-compare is timestamp-driven and incremental is a simple append.


Additional refactoring checklist
- Run/trace identifiers ... Should the notebook generate one run_id (UUID) at start and attach to metrics and event rows? 
