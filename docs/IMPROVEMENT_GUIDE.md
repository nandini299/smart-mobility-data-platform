# Smart Mobility Data Platform: Improvement Guide

A self-contained plan to take the project from "clean mid-level" (~6.5–7/10) to
"senior-ready" (~8–8.5/10) for 3-year data engineer roles.

**How to use this guide**

- Do the tasks in order. Each one builds on the previous.
- Every task has the same sections: **Why** → **Steps** → **Verify** → **Write in the README** → **Interview questions it prepares you for** → **Pitfalls**.
- Code here is a *skeleton*: correct in structure, but check names and versions against the official docs (links at the end) before relying on it. Reading those docs is part of the learning.
- **Keep this guide private.** Don't commit it to the repo.
- **Commit as you go:** one branch per task, small commits, and a PR into `main` each time. A history that grows over weeks is itself evidence.

**Time budget (part-time):** Tier 1 ≈ 2–3 weeks, Tier 2 ≈ 1–2 weeks.

---

## Task 0: Safety first (30 minutes, do this before anything else)

You're about to scan much more data on AWS. Protect your wallet.

**Steps**
1. **AWS Budgets:** in the Billing console, create a monthly cost budget (e.g. $10) with email alerts at 50%, 80% and 100%.
2. **Athena workgroup scan limit:** create a workgroup `mobility` with *Data usage controls → per-query limit* of 1 GB. Task 1 recreates this in Terraform; do it by hand now so you're protected today.
3. **S3 lifecycle rule** on `athena-results/`: expire objects after 7 days. Query results pile up quietly.
4. Set `ATHENA_WORKGROUP=mobility` in your `.env`. The dashboards already read it.

**Verify:** run an Athena query with no partition filter on a big table. It should fail with a "bytes scanned limit exceeded" error. That's what you want.

**Interview angle:** "How do you control cost on a pay-per-query engine?" You now have a concrete answer: workgroup limits, partition pruning, columnar formats and result caching.

---

# TIER 1: moves you from mid-level to senior-ready

## Task 1: Infrastructure as Code with Terraform

**Why.** Right now nobody, including future you, can recreate your AWS setup: bucket, Glue databases, tables and IAM were clicked together in the console. At 3 years, interviewers expect you to manage infrastructure as code. It's also the only way to make your project reproducible.

**What to put in Terraform (scope it; don't boil the ocean)**

| Resource | Terraform type |
|---|---|
| Data lake bucket (+ public access block, encryption, versioning, lifecycle) | `aws_s3_bucket`, `aws_s3_bucket_public_access_block`, `aws_s3_bucket_server_side_encryption_configuration`, `aws_s3_bucket_versioning`, `aws_s3_bucket_lifecycle_configuration` |
| Glue databases `mobility_silver`, `mobility_gold` | `aws_glue_catalog_database` |
| Silver tables (with partition projection, see Task 3) | `aws_glue_catalog_table` |
| Athena workgroup with scan limit | `aws_athena_workgroup` |
| IAM role for Glue jobs | `aws_iam_role`, `aws_iam_role_policy` |
| Glue jobs (Task 4) | `aws_glue_job` |
| Budget | `aws_budgets_budget` |

Leave Gold tables to dbt; dbt creates them. Infrastructure belongs in Terraform, data models in dbt. That split is a good interview point by itself.

**Steps**

1. Install Terraform and create `infra/` in the repo:
   ```
   infra/
     main.tf          # provider + backend
     variables.tf
     s3.tf
     glue_catalog.tf
     athena.tf
     iam.tf
     glue_jobs.tf     # Task 4
     outputs.tf
     terraform.tfvars.example
   ```
   `.gitignore` already ignores `*.tfstate`, `.terraform/` and `terraform.tfvars`.

2. **Provider and variables** (`main.tf`, `variables.tf`):
   ```hcl
   terraform {
     required_version = ">= 1.6"
     required_providers {
       aws = { source = "hashicorp/aws", version = "~> 5.0" }   # check the current major
     }
   }

   provider "aws" {
     region = var.region
     default_tags { tags = { project = "smart-mobility", managed_by = "terraform" } }
   }

   variable "region"      { default = "ap-south-1" }
   variable "bucket_name" { type = string }
   ```

3. **Bucket** (`s3.tf`): bucket + block all public access + SSE-S3 encryption + versioning + lifecycle rules (expire `athena-results/` after 7 days, and optionally move `bronze/` to a cheaper storage class after 30 days).

4. **Athena workgroup** (`athena.tf`):
   ```hcl
   resource "aws_athena_workgroup" "mobility" {
     name = "mobility"
     configuration {
       enforce_workgroup_configuration    = true
       bytes_scanned_cutoff_per_query     = 1073741824   # 1 GB (minimum allowed is 10 MB)
       publish_cloudwatch_metrics_enabled = true
       result_configuration {
         output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
       }
     }
   }
   ```

5. **Glue catalog** (`glue_catalog.tf`): two databases, plus the Silver tables. Define columns to match the Silver Parquet schema exactly; look at `FINAL_COLUMNS` in `glue/scripts/trips_bronze_to_silver.py`. Partition projection settings come in Task 3.

6. **IAM for Glue** (`iam.tf`): a role that Glue can assume (`glue.amazonaws.com`), with:
   - the AWS-managed `AWSGlueServiceRole` policy attached
   - an inline policy for `s3:GetObject`/`PutObject`/`DeleteObject`/`ListBucket` on **your bucket only**

   Least privilege: never `s3:*` on `*`.

7. **Import what already exists.** Your bucket and databases already exist, so `terraform apply` would try to create duplicates. Use `import` blocks (Terraform ≥ 1.5) or `terraform import` so Terraform adopts them:
   ```hcl
   import {
     to = aws_s3_bucket.lake
     id = "your-bucket-name"
   }
   ```
   Then `terraform plan` should show **no destroy and no replace** for the bucket. Read the plan carefully every time.

8. **Remote state (optional but impressive):** store state in S3 with locking. Mention it in the README even if you keep local state, and explain the trade-off.

**Verify**
- `terraform fmt -check` and `terraform validate` pass.
- `terraform plan` shows the changes you expect and nothing else.
- Add both commands to CI (`hashicorp/setup-terraform` action). Don't run `apply` in CI for a personal project.

**Write in the README:** a short "Infrastructure" section listing what Terraform manages, how to run it (`cp terraform.tfvars.example terraform.tfvars && terraform init && terraform apply`), and the infra-vs-dbt split.

**Interview questions**
- "What is Terraform state, and what happens if two people apply at once?" (state locking)
- "How did you bring existing resources under Terraform?" (import)
- "Why not `s3:*`?" (least privilege, blast radius)

**Pitfalls**
- Never commit `terraform.tfstate`. It can contain secrets.
- Deleting a resource block deletes the resource on the next apply. Always read the plan.
- Inside Terraform strings, `${` is interpolation. Write `$${` when you need a literal `${`, as in Task 3.

---

## Task 2: Scale up the data, and measure everything

**Why.** "I processed N rows" is worth little. "Here's what broke at scale, how I fixed it, and the before/after numbers" is worth a lot. Scale is how you generate those stories.

**Steps**

1. **Choose the scope:** yellow taxi **2019–2024** (6 years, roughly hundreds of millions of rows). Optionally add the high-volume FHV (Uber/Lyft) files later. They're larger per month, with a different schema.

2. **Record a baseline first.** Before changing anything, write down for one month and for all months:
   - Glue job wall-clock time per month
   - Silver output size and file count per partition
   - dbt run time for `fact_trips`
   - Athena data scanned and runtime for 3–4 typical dashboard queries (see the query history, or the `-- dashboard: <name>` comments in the new dashboard)

   Keep a `docs/benchmarks.md` table. **You can't claim an improvement you didn't measure.**

3. **Backfill through Airflow:** change `trips_monthly` `start_date`/`end_date` to cover the new range and keep `max_active_runs=1` for the first run. Then try `max_active_runs=3`. Is it safe? It is only if each run touches only its own partitions. Check: Glue writes with dynamic partition overwrite, and dbt uses `insert_overwrite` on `pickup_month`. Write down *why* parallel runs are safe. That reasoning is the interview answer.

4. **Expect schema drift; this is the real lesson.** TLC files change over the years: column names, letter case and types (e.g. a column stored as double in one year and integer in another; columns added in later years). Steps:
   - Download a few months from different years and print their schemas with `pyarrow.parquet.read_schema(path)`.
   - Make a table in `docs/`: column × year → name and type.
   - Change `rename_and_cast_trips` so it handles *every* variant: rename by a mapping that ignores case, add missing columns as typed nulls, and cast to the Silver type. Fail loudly on an **unknown** column instead of silently dropping it.
   - **Add unit tests** with a DataFrame per schema variant. This is exactly the kind of test interviewers like.

5. **Watch for, then fix:**
   - **Small files** in Silver (count files per partition)
   - **Skew** (one partition much bigger than the rest)
   - **Slow Athena queries**

   Record each fix with numbers.

**Verify:** the backfill completes, dbt tests pass on all months, and `docs/benchmarks.md` has a before and after for at least 3 changes.

**Write in the README:** a "Scale & performance" section with the benchmark table. Something like: *"6 years, X M rows. Backfill: A → B after <change>. Dashboard query scan: C GB → D MB after <change>."* Only real numbers.

**Interview questions**
- "How do you handle schema evolution in a lake?"
- "Is your backfill idempotent? Can it run in parallel?"
- "How did you find and fix the bottleneck?"

**Pitfalls**
- Turn on the budget alarm (Task 0) first.
- Don't load everything and *then* start measuring. Baseline first.

---

## Task 3: Partition projection instead of `MSCK REPAIR`

**Why.** `MSCK REPAIR TABLE` lists every S3 prefix on every run, so it gets slower as partitions grow. With partition projection, Athena *computes* partitions from rules, so no partitions need registering at all and query planning gets faster.

**Steps**

1. In the Silver table definition (Terraform, Task 1), add table parameters. For `trips`, partitioned by `pickup_date`:
   ```hcl
   parameters = {
     "projection.enabled"             = "true"
     "projection.pickup_date.type"    = "date"
     "projection.pickup_date.range"   = "2019-01-01,NOW"
     "projection.pickup_date.format"  = "yyyy-MM-dd"
     "projection.pickup_date.interval"      = "1"
     "projection.pickup_date.interval.unit" = "DAYS"
     "storage.location.template" = "s3://${var.bucket_name}/silver/trips/pickup_date=$${pickup_date}/"
   }
   ```
   Note `$${pickup_date}`: Terraform has to output a literal `${pickup_date}` for Athena.

2. Do the same for `sensor_readings` on `reading_date`.

3. Remove the `register_silver_partitions` tasks (`MSCK REPAIR`) from both DAGs.

4. Measure query planning and run time for the same query, before and after.

**Verify:** a query filtered on one date returns data without any repair step, and `SHOW PARTITIONS` isn't needed anymore.

**Interview questions:** "MSCK REPAIR vs. ALTER TABLE ADD PARTITION vs. projection vs. a Glue crawler: when do you use which?"

**Pitfalls**
- A projected partition with no data simply returns no rows, so a broken path template fails *silently*. Test with a known date.
- Projection applies to Athena only. Other engines reading the Glue catalog don't see projected partitions.

---

## Task 4: Run the Glue jobs as real AWS Glue jobs

**Why.** Today the jobs run in the Glue Docker image on your machine. That's fine for development, but then "built AWS Glue jobs" on your resume isn't strictly true. Running them on the managed service even a few times makes it true, and teaches you DPUs, job metrics and bookmarks.

**Steps**

1. Upload scripts to S3 (`s3://bucket/glue/scripts/`). Terraform can do this with `aws_s3_object` using `etag = filemd5(...)` so changes re-upload.

2. Define the job in Terraform:
   ```hcl
   resource "aws_glue_job" "trips_bronze_to_silver" {
     name              = "trips_bronze_to_silver"
     role_arn          = aws_iam_role.glue.arn
     glue_version      = "4.0"
     worker_type       = "G.1X"
     number_of_workers = 2
     timeout           = 60   # minutes
     command {
       script_location = "s3://${var.bucket_name}/glue/scripts/trips_bronze_to_silver.py"
       python_version  = "3"
     }
     default_arguments = {
       "--bucket"                           = var.bucket_name
       "--enable-metrics"                   = "true"
       "--enable-continuous-cloudwatch-log" = "true"
     }
   }
   ```

3. In the DAG, swap the Docker `BashOperator` for `GlueJobOperator` from `airflow.providers.amazon.aws.operators.glue`, with `job_name`, `script_args={"--year": "...", "--month": "..."}` and `wait_for_completion=True`.

   Keep the Docker path available too, for example with an Airflow Variable `GLUE_MODE=local|aws`, so development stays free.

4. Run one month, then check run time, DPU-hours and cost in the Glue console. Write them down.

**Verify:** the managed run produces the same Silver row counts as your local run for the same month.

**Write in the README:** both run modes and the cost per monthly run.

**Interview questions**
- "What is a DPU, and how did you size the job?"
- "Glue vs. EMR vs. EMR Serverless: when do you use each?"
- "What are Glue job bookmarks, and why didn't you need them?" (Your jobs are partition-scoped and idempotent, so bookmarks would add hidden state for no benefit. That's a strong answer.)

**Pitfalls**
- Two workers are plenty for one month of taxi data. Don't over-provision.
- Delete test jobs you don't use.

---

## Task 5: Make data quality *gate* the pipeline, and alert

**Why.** Today you write DQ metrics to S3 and nothing reads them. A data quality check that never stops bad data from reaching Gold is just a log line. In production, the pipeline must stop and someone must be told.

**Steps**

1. **Threshold check task** after each Glue job. A `PythonOperator` that:
   - reads `s3://bucket/dq_metrics/trips/run_month=YYYY-MM/*.json` with boto3
   - computes `removed_ratio = (input_rows - output_rows) / input_rows`
   - raises `AirflowFailException` (from `airflow.exceptions`) if it's above a threshold, e.g. 5%, so the run fails **without retrying**
   - also fails if `output_rows == 0`

   Put thresholds in Airflow Variables, not in code.

   Do the same for sensors, plus a **dead-letter rate** check (dead-letter rows ÷ total rows per day).

2. **dbt source freshness** in `models/staging/sources.yml`:
   ```yaml
   tables:
     - name: sensor_readings
       loaded_at_field: event_ts
       freshness:
         warn_after:  {count: 26, period: hour}
         error_after: {count: 48, period: hour}
   ```
   Run `dbt source freshness` as the first dbt task in `sensors_daily`. In recent dbt versions the freshness config may need to sit under `config:`; check the docs for your version.

3. **Real alerting:** replace `print` in `notify_failure` with a Slack incoming webhook (a simple `requests.post`) or AWS SNS email (`boto3.client("sns").publish`). Keep the webhook URL in an Airflow Connection or Variable, **never in code**. Include DAG, task, run date and the log URL (`context["task_instance"].log_url`).

4. **Prove it works:** deliberately break something, e.g. make the simulator send 30% malformed messages. Confirm the DAG stops before dbt and you get the alert. Screenshot it for the README.

**Verify:** bad data stops the DAG before the Gold layer, and an alert arrives.

**Write in the README:** a "Data quality & alerting" section: what's checked, the thresholds, what happens on failure, and the screenshot.

**Interview questions**
- "What happens when bad data arrives?"
- "Why fail the pipeline instead of filtering more?"
- "What's the difference between a dbt test and a freshness check?"

**Pitfalls**
- Thresholds too tight cause alert fatigue; too loose and they're useless. Base them on the data you've seen (Task 2 gives you the history) and write down why you chose them.

---

# TIER 2: depth and storytelling

## Task 6: Answer a real business question (join trips and sensors)

**Why.** Right now there are two unrelated datasets side by side. A platform should answer a question. One clear answer with a chart is your demo's punchline.

**Important problem first:** your trips are from **2023** (or 2019–2024 after Task 2), but your sensor data is from **2026**, when you ran the simulator. Joining them by time gives **zero rows**.

**Fix:** add a **historical backfill mode** to the simulator that generates sensor events for past timestamps and writes them through the same path:
- `python streaming/sensor_simulator.py --backfill-start 2023-01-01 --backfill-end 2023-12-31 --step-seconds 60`
- Instead of `time.time()`, loop over simulated timestamps and set `event_ts_ms` from them.
- Don't sleep in backfill mode; send as fast as Kafka accepts.
- Make the simulated values depend on real trip patterns (e.g. traffic count follows hourly trip volume for that zone) so the join shows a real relationship, and **say in the README that the sensor data is simulated.**

**Then build the mart:** `models/marts/fct_zone_hourly_conditions.sql`.
- Grain: one row per **zone per hour**.
- Columns: trips started in the zone that hour (from `fact_trips`), plus that hour's average traffic count, CO, temperature and visibility for the zone (from `fact_sensor_hourly` joined via `dim_sensor.zone_id`).
- Add a grain uniqueness test.

**The question to answer** (pick one):
- "Is trip demand lower in hours with poor visibility?"
- "Do high-CO hours coincide with rush-hour demand in Midtown?"

Show the answer in the sensors dashboard (a scatter of trips vs. visibility per zone-hour) and state it in one sentence in the README, with the caveat that the sensors are simulated.

**Interview questions**
- "How did you choose the grain?"
- "What would you change if sensors reported at irregular intervals?" (time bucketing, as-of joins)

---

## Task 7: Streaming correctness

**Why.** Your stream works, but interviewers probe streaming hardest because it's where systems lose or duplicate data.

**Steps**

1. **Checkpoints on S3:** set `checkpointLocation` to `s3a://bucket/checkpoints/sensors/` instead of a local folder. A local checkpoint is lost when the machine or container dies, so the stream restarts from scratch.

2. **`startingOffsets`:** document the choice. `latest` skips anything sent while the stream was down *on first start only*; after that, the checkpoint wins. For a new deployment, `earliest` is usually safer.

3. **Reconsider `failOnDataLoss=false`.** It hides data loss when Kafka retention deletes unread data. Set it to `true` in "production" mode, or keep it and add an alert. Either way, write down the decision.

4. **Late data and in-stream dedup** (Spark 3.5):
   ```python
   good = (good
       .withWatermark("event_ts", "10 minutes")
       .dropDuplicatesWithinWatermark(["event_id"]))
   ```
   Explain the trade-off: dedup in the stream reduces Bronze duplicates; dedup in Silver is still the guarantee.

5. **Measure throughput:** raise the simulator rate (more sensors, shorter interval) until the stream falls behind. Read `query.lastProgress` (`inputRowsPerSecond` vs `processedRowsPerSecond`) and record the maximum sustained rate. Note: Spark tracks Kafka offsets in its *checkpoint*, not as consumer-group commits, so Kafka's consumer-group lag tools won't show it. Use the progress metrics instead.

6. **Write down the delivery guarantees:**
   - Kafka → Bronze is effectively *exactly once per checkpointed batch* for file sinks, but the producer can create duplicates (as your simulator does on purpose).
   - Silver dedup by `event_id` gives *effectively-once* results end to end.

   Say precisely which guarantee holds where.

**Interview questions**
- "What happens if the stream crashes mid-batch?"
- "Explain watermarks."
- "At-least-once vs. exactly-once: what does your pipeline guarantee, and where?"

---

## Task 8: Tests that matter

1. **Make the sensors Glue job testable.** It runs at module level today. Restructure it like the trips job:
   - `clean_sensor_readings(df) -> DataFrame` (dedup, filter, select)
   - `main()` reads args, reads, writes and records metrics
   - `if __name__ == "__main__": main()`

   Then add `tests/glue/test_sensors_bronze_to_silver.py`: duplicates removed, negative values removed, columns renamed.

2. **Schema-drift tests** for trips (from Task 2): one test per schema variant.

3. **dbt unit tests** (dbt ≥ 1.8): test the *logic* of a model with fixed input rows, without real data:
   ```yaml
   unit_tests:
     - name: fact_trips_date_key_and_month
       model: fact_trips
       overrides:
         macros:
           is_incremental: false
       given:
         - input: ref('stg_trips')
           rows:
             - {trip_id: "t1", pickup_date: "2023-01-10", pickup_ts: "2023-01-10 10:00:00"}
       expect:
         rows:
           - {trip_id: "t1", date_key: 20230110, pickup_month: "2023-01"}
   ```
   Good candidates: `fact_trips` (date key, month), `fact_sensor_hourly` (the point-in-time join picks the right sensor version), `dim_sensor` (`is_current`). Unit tests run against your warehouse connection, so run them locally, not in CI without credentials.

4. **Test the dashboard offline** with Streamlit's `AppTest` (`streamlit.testing.v1`) and a fake query function that returns small DataFrames. It checks the app runs without errors and that filters produce the right query parameters.

**Interview question:** "How do you test a data pipeline?" Your answer can now be layered: unit tests for transforms, dbt unit tests for model logic, dbt data tests for data, DQ gates in production, and alerts.

---

## Task 9: Architecture Decision Records (ADRs)

**Why.** This shows judgement, which is what separates 3 years from 1 year. Each record is one page.

Create `docs/adr/` with 4–6 files using this template:

```markdown
# ADR-001: Use dbt insert_overwrite for incremental facts

Status: Accepted · Date: 2026-10-20

## Context
What problem, what constraints (Athena, cost, idempotent reruns).

## Options considered
1. merge  2. append  3. insert_overwrite  4. full refresh

## Decision
insert_overwrite on pickup_month.

## Consequences
+ Reruns are idempotent; cost scales with one month.
- Late-arriving trips for an old month need that month re-run.
```

**Topics:**
1. medallion on S3 + Athena vs. a warehouse
2. `insert_overwrite` vs. `merge`
3. SCD2 for sensors
4. dedup in Silver vs. in the stream
5. local Glue runtime vs. managed Glue
6. Hive tables vs. Iceberg

For the last one, it's fine to conclude "Iceberg later, because X". Knowing *when* to adopt Iceberg (ACID merges, schema evolution, time travel, small-file compaction) is a strong signal even if you don't adopt it.

---

## Task 10: Small polish items (a day in total)

- **CI action versions:** bump `actions/checkout` and `actions/setup-python` to their current major versions (check each action's GitHub releases page). This clears the Node 20 deprecation warnings.
- **Pin dependency versions** in `requirements.txt`, or move to `uv`/`pip-tools` with a lock file. Unpinned dependencies break reproducibility.
- **`.env` loading:** make sure every entry point (simulator, stream, DAGs, dashboards) reads config the same way.
- **README screenshots** of each dashboard tab, the Airflow DAG graph, and a DQ alert.
- **Remove the `group_add` default** if you move to a setup that doesn't need the Docker socket (e.g. managed Glue only).

---

# Working practices (these are visible to reviewers)

**Git**
- One branch per task (`feat/terraform`, `feat/partition-projection`, ...), with a PR into `main` each time, even working alone.
- Small commits with clear messages, e.g. `feat(glue): handle 2019 schema variant`, `fix(dag): ...`, `docs: ...`, `test: ...`.
- The PR description says what changed, why, and how you tested it. That's what your work at a company should look like too.

**Docs**
- The README is the front page: keep it current after every task.
- `docs/benchmarks.md` holds the numbers; `docs/adr/` holds the decisions.

**Honesty**
- Say what's simulated (sensors) and what's real (TLC trips).
- Say what runs locally vs. on AWS.
- Never put a number in the README or on your resume that you didn't measure.

---

# Resume bullets (fill in your real numbers)

Write these only after doing the work, with measured numbers:

- Built a batch + streaming lakehouse on AWS (S3, Glue, Athena) orchestrated by Airflow, processing **[N] M** NYC taxi trips across **[Y]** years plus a simulated IoT sensor stream via Kafka and Spark Structured Streaming.
- Modelled Gold star schemas in dbt with incremental `insert_overwrite` partitions and an SCD Type 2 sensor dimension with point-in-time joins; enforced grain and referential integrity with **[N]** dbt tests.
- Cut dashboard query scan from **[X] GB to [Y] MB** and backfill time from **[A] to [B]** through partition projection and **[your fix]**.
- Provisioned infrastructure with Terraform (S3, Glue, Athena workgroups with scan limits, least-privilege IAM), and added data-quality gates with Slack/SNS alerting that stop bad data before the Gold layer.

Rules: start with a verb, one line each, numbers where real, and no tool you can't explain.

---

# Interview preparation

## Questions about *your* project; practise answering out loud
1. Walk me through the architecture in 2 minutes.
2. Why Kafka and Spark for ~10 events per second? (To learn the pattern; at this volume a simple consumer or Firehose would do. Say what you'd use and why.)
3. What happens if the Glue job dies halfway through writing? (Dynamic partition overwrite + a rerun rewrites only those partitions, so the job is idempotent.)
4. How do you avoid duplicate trips? (Deterministic sha2 `trip_id` + dedup; what about two genuinely identical trips?)
5. Explain your SCD2 dimension and the point-in-time join. What breaks if validity windows overlap? (Fan-out, which is why you have the grain test.)
6. What does your pipeline guarantee: at-least-once or exactly-once? Where?
7. How would this look at 100x the data? (Iceberg, compaction, EMR Serverless, more partitions, cost controls.)
8. What would you do differently if you started again?
9. How is the LLM-to-SQL feature secured? (AST validation + allow-list + read-only IAM + workgroup scan limit.)
10. What did you measure, and what did you improve?

## Core topics for the 3-year level
- **SQL:** window functions (`row_number`, `lag`, running totals), dedup patterns, gaps and islands, `GROUP BY` vs. window aggregation, reading a query plan.
- **Spark:** partitions vs. files, shuffles, skew (salting, AQE skew join), broadcast joins, `cache` and when not to use it, narrow vs. wide transformations.
- **Data modelling:** star vs. snowflake, choosing the grain, SCD types 1/2, fact types (transaction, periodic snapshot, accumulating).
- **Orchestration:** idempotency, backfills, `catchup`, retries vs. fail-fast, data intervals (`ds` vs. `logical_date`).
- **Lakehouse:** Parquet internals (row groups, column pruning, predicate pushdown), small files, table formats (Iceberg/Delta/Hudi) and why they exist.
- **System design:** practise "Design a pipeline for X at Y events per second". Cover ingestion, storage layers, processing, serving, quality, monitoring and cost.

---

# Official docs to keep open

- Terraform AWS provider: registry.terraform.io/providers/hashicorp/aws/latest/docs
- Athena partition projection: docs.aws.amazon.com/athena (search "partition projection")
- AWS Glue job parameters: docs.aws.amazon.com/glue (search "job parameters")
- Airflow Amazon provider (Glue and Athena operators): airflow.apache.org/docs/apache-airflow-providers-amazon
- dbt docs: docs.getdbt.com (unit tests, source freshness, incremental models)
- dbt-athena adapter: docs.getdbt.com (search "athena configs")
- Spark Structured Streaming guide: spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- NYC TLC data dictionary: nyc.gov/site/tlc (search "trip record data")

---

# Checklist

- [ ] 0. Budget alarm + Athena workgroup limit + results lifecycle
- [ ] 1. Terraform: bucket, catalog, workgroup, IAM (existing resources imported)
- [ ] 2. 6-year backfill, schema-drift handling + tests, `docs/benchmarks.md`
- [ ] 3. Partition projection; MSCK removed; before/after measured
- [ ] 4. Glue jobs run on managed Glue (cost recorded)
- [ ] 5. DQ gates + freshness + Slack/SNS alerts (failure demo screenshot)
- [ ] 6. Sensor backfill mode + `fct_zone_hourly_conditions` + one answered question
- [ ] 7. Streaming: S3 checkpoints, watermark dedup, throughput measured, guarantees documented
- [ ] 8. Sensors job tests, dbt unit tests, dashboard AppTest
- [ ] 9. 4–6 ADRs
- [ ] 10. Polish: action versions, pinned deps, screenshots
- [ ] README updated after every task · resume bullets with real numbers
