# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2b598e5c-9cff-497e-b43b-7a5d1b91c0df",
# META       "default_lakehouse_name": "gold_lh",
# META       "default_lakehouse_workspace_id": "7e206237-aef1-4932-9f94-1f6ae343407a"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Shared logging notebook. Invoked two ways (see header comment in the next
# cell for why notebookutils.notebook.run() was chosen over %run):
#   1. By every other notebook in this repo, on ITS OWN success path only,
#      via notebookutils.notebook.run("pipeline_log", ..., {...}).
#   2. Directly by the pipeline as a dedicated LogXFailure activity
#      (dependsOn: Failed) whenever a stage's own notebook activity fails —
#      the pipeline supplies status="Failed" and a real error_message in
#      that case. This notebook has no special-casing for that path; it
#      just writes whatever row it's told to.
#
# Defaults below let this notebook run standalone (e.g. via
# `python infra/deploy_notebook.py notebooks/nb_pipeline_log.py --run
# --param status=Failed --param error_message="manual test"`) without a
# caller notebook.
run_id = "manual"
stage = ""
status = "Succeeded"
start_ts = ""
end_ts = ""
rows_read = 0
rows_written = 0
error_message = ""
run_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Mechanism choice: notebookutils.notebook.run() rather than %run.
# %run inlines the callee's cells into the caller's namespace at edit/parse
# time and has no clean way to pass a parameters map into a callee that
# declares its own PARAMETERS CELL (it's built for interactive composition,
# not parameterized invocation). notebookutils.notebook.run(path,
# timeoutSeconds, arguments) runs the callee as an isolated child job with
# its own parameter binding — exactly the same RunNotebook mechanism the
# pipeline itself uses for LogXFailure activities (see
# infra/fabric/common.run_notebook). Using one mechanism everywhere (every
# caller of this notebook is really just triggering a RunNotebook job, one
# way or another) means there's a single mental model for "how does logging
# happen", not two.
#
# error_message is passed through as "" when there is none (Fabric notebook
# parameters don't have a clean way to pass a real None), and normalized to
# SQL NULL here, not stored as an empty string, so a query for
# `error_message IS NULL` behaves as expected.
#
# start_ts/end_ts arrive as ISO-8601 strings (notebook parameters are plain
# str/int/bool — no native datetime parameter type). Parsed here into real
# datetimes and then explicitly .cast("timestamp") after DataFrame creation
# — belt-and-suspenders against the timestamp_ntz-invisible-on-SQL-endpoint
# gotcha (see CLAUDE.md "Notebook deploy/run mechanics"): a bare Python
# datetime with tzinfo already produces classic TimestampType, not NTZ, but
# the explicit cast costs nothing and matches the documented safe pattern.
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

if status not in ("Succeeded", "Failed"):
    raise ValueError(f"status must be 'Succeeded' or 'Failed', got {status!r}")

_now = datetime.now(timezone.utc)
_start_ts = datetime.fromisoformat(start_ts) if start_ts else _now
_end_ts = datetime.fromisoformat(end_ts) if end_ts else _now
_error_message = error_message if error_message else None
if not run_date:
    run_date = _now.strftime("%Y-%m-%d")

# Explicit schema, not inferred: error_message is None whenever the caller
# reports success, and createDataFrame can't infer a type for a column with
# no non-null sample value in a single-row frame ([CANNOT_DETERMINE_TYPE],
# confirmed live in the sibling banking POC — same root cause here).
CONTROL_LOG_SCHEMA = StructType([
    StructField("run_id", StringType()),
    StructField("stage", StringType()),
    StructField("status", StringType()),
    StructField("start_ts", TimestampType()),
    StructField("end_ts", TimestampType()),
    StructField("rows_read", LongType()),
    StructField("rows_written", LongType()),
    StructField("error_message", StringType()),
    StructField("run_date", StringType()),
])

control_row = Row(
    run_id=run_id,
    stage=stage,
    status=status,
    start_ts=_start_ts,
    end_ts=_end_ts,
    rows_read=int(rows_read),
    rows_written=int(rows_written),
    error_message=_error_message,
    run_date=run_date,
)

control_df = (
    spark.createDataFrame([control_row], schema=CONTROL_LOG_SCHEMA)
    .withColumn("start_ts", F.col("start_ts").cast("timestamp"))
    .withColumn("end_ts", F.col("end_ts").cast("timestamp"))
)

# gold_lh is this notebook's default lakehouse (see METADATA above), so a
# plain saveAsTable works — no cross-lakehouse abfss path needed. append
# mode creates the table on first write if it doesn't exist yet.
control_df.write.format("delta").mode("append").saveAsTable("control_pipeline_run_log")

print(f"control_pipeline_run_log: appended run_id={run_id} stage={stage!r} status={status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
