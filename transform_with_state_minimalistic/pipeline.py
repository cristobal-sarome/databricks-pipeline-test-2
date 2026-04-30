# Minimalistic pipeline to test transformWithState
# Single step: Auto Loader → parse → transformWithState → Delta Lake

from datetime import datetime, timezone

from pyspark import pipelines as dp
from pyspark.sql import Row, functions as F
from pyspark.sql.streaming.stateful_processor import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StringType, StructField, StructType

# ── Config ────────────────────────────────────────────────────────────────────

INPUT_PATH   = "abfss://logs@yoizenftplanding.dfs.core.windows.net/"
FILTRO       = "*yflow-claroco*"
_TTL_MS      = 24 * 60 * 60 * 1000  # 24 h of inactivity

# Minimal schema — only the fields we actually use
_SCHEMA = StructType([
    StructField("message", StringType(), True),
])


# ── State processor ───────────────────────────────────────────────────────────

class _BlockTracker(StatefulProcessor):
    """Carry the last navigation block forward to every event in a case session."""

    def init(self, handle: StatefulProcessorHandle) -> None:
        self._last_block = handle.getValueState(
            "last_block",
            StructType([StructField("v", StringType())]),
            ttlDurationMs=_TTL_MS,
        )

    def handleInputRows(self, key, rows, timerValues):
        state = self._last_block.get()
        running_block = (state.v if state and state.v is not None else "Unknown")

        for row in sorted(
            rows,
            key=lambda r: r.ts_utc if r.ts_utc is not None
                          else datetime.min.replace(tzinfo=timezone.utc),
        ):
            if row.event_type == "navigation" and row.nav_bloque:
                running_block = row.nav_bloque
            yield Row(**{**row.asDict(), "active_block": running_block})

        state_now = self._last_block.get()
        last_saved = state_now.v if state_now else None
        if running_block != last_saved:
            self._last_block.update(Row(v=running_block))

    def close(self) -> None:
        pass


# ── Pipeline table ────────────────────────────────────────────────────────────

@dp.table(
    name="test_tws.events_with_block",
    comment="Minimal test: Auto Loader → parse → transformWithState → Delta",
    partition_cols=["dia_real"],
)
def events_with_block():
    # ── 1. Ingest via Auto Loader ─────────────────────────────────────────────
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.maxFilesPerTrigger", "50")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("pathGlobFilter", FILTRO)
        .schema(_SCHEMA)
        .load(INPUT_PATH)
    )

    # ── 2. Minimal parse ──────────────────────────────────────────────────────
    parsed = (
        raw
        .withColumn("case_id",    F.get_json_object(F.col("message"), "$.payload.caseId"))
        .withColumn("ts_utc",     F.get_json_object(F.col("message"), "$.time").cast("timestamp"))
        .withColumn("msg_text",   F.get_json_object(F.col("message"), "$.msg"))
        .withColumn("dia_real",
            F.date_format(
                F.from_utc_timestamp(
                    F.get_json_object(F.col("message"), "$.time").cast("timestamp"),
                    "America/Bogota",
                ),
                "yyyy-MM-dd",
            )
        )
        .withColumn("event_type",
            F.when(F.col("msg_text").contains("Bloque actual:"),          F.lit("navigation"))
            .when(F.col("msg_text").contains("Se invocó a la integración"), F.lit("integration"))
        )
        .withColumn("nav_bloque",
            F.when(
                F.col("event_type") == "navigation",
                F.trim(F.regexp_extract(F.col("msg_text"), r"Bloque actual:\s*(.+)$", 1)),
            )
        )
        .filter(F.col("case_id").isNotNull() & F.col("event_type").isNotNull())
    )

    # ── 3. transformWithState — attach active block to every event ────────────
    output_schema = StructType(
        parsed.schema.fields + [StructField("active_block", StringType(), True)]
    )

    return (
        parsed
        .groupBy("case_id")
        .transformWithState(
            _BlockTracker(),
            outputMode="append",
            timeMode="none",
            outputStructType=output_schema,
        )
    )
