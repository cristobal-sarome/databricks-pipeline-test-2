# Gold — Unified fact table fLog_Maestro
#
# Materialized view (batch read from silver) to support the stateful block-tracking
# window function: for each event, we propagate the most recently seen navigation
# block forward in time within the same case_id.
#
# Trade-off: full recompute on every pipeline trigger. The partitionBy("case_id")
# on the window keeps this parallel. If cost grows, add a dia_real date filter here
# to limit recompute to a rolling window (e.g., last 7 days).

from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window

# ── Gold table ────────────────────────────────────────────────────────────────

@dp.table(
    name="gold.flog_maestro",
    comment="Unified fact table consumed by BI/dashboards. Maps all event types to a "
            "shared schema. Integrations inherit the active flow block via window function.",
    partition_cols=["dia_real"],
)
@dp.expect("has_case_id", "CaseId IS NOT NULL")
def flog_maestro():
    silver = dp.read("silver.parsed_events")

    # ── Stateful block tracking ───────────────────────────────────────────────
    # For each row in a case (ordered by timestamp), carry the last known
    # navigation block forward. Integration events inherit whatever block was
    # active at their timestamp. "Desconocido" when no prior navigation exists.
    block_window = (
        Window
        .partitionBy("case_id")
        .orderBy(F.col("ts_utc").asc())
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    silver = silver.withColumn("_active_block",
        F.coalesce(
            F.last(
                F.when(F.col("event_type") == "navigation", F.col("nav_bloque")),
                ignorenulls=True,
            ).over(block_window),
            F.lit("Desconocido"),
        )
    )

    # ── Exclude events that go to the 3_Mensajes category (not consumed) ──────
    # Also drop variable rows that are neither messages nor valid context variables
    # (i.e., variables in the ignore list like bodyLength, accessToken, etc.)
    et = F.col("event_type")
    silver = silver.filter(
        ~F.col("var_es_mensaje")
        & ((et != "variable") | F.col("var_es_contexto"))
    )

    # ── Column expressions for the shared schema ──────────────────────────────

    tabla = (
        F.when(et == "navigation",   F.lit("1_Navegacion"))
        .when(et == "integration",   F.lit("2_Integraciones"))
        .when(et == "variable",      F.lit("4_Contexto"))
        .when(et == "context",       F.lit("4_Contexto"))
        .when(et == "engine_error",  F.lit("5_Errores_Motor"))
    )

    bloque = (
        F.when(et == "navigation",   F.col("nav_bloque"))
        .when(et == "integration",   F.col("_active_block"))
        .when(et == "engine_error",  F.lit("Error de Sistema"))
        .otherwise(F.lit("Sin Registro de Bloque"))
    )

    concepto = (
        F.when(et == "navigation",   F.col("nav_evento"))
        .when(et == "integration",   F.col("int_nombre"))
        .when(et == "variable",      F.col("var_nombre"))
        .when(et == "context",       F.col("ctx_variable"))
        .when(et == "engine_error",  F.col("err_type"))
    )

    valor_detalle = (
        F.when(et == "navigation",   F.col("nav_bloque"))
        .when(et == "integration",
            F.concat(
                F.coalesce(F.col("int_duracion_ms").cast("string"), F.lit("0")),
                F.lit(" ms"),
            )
        )
        .when(et == "variable",      F.col("var_valor"))
        .when(et == "context",       F.col("ctx_valor"))
        .when(et == "engine_error",  F.col("err_detalle"))
    )

    estado = (
        F.when(et == "navigation",   F.col("log_level"))
        .when(et == "integration",   F.col("int_status"))
        .when(et == "variable",      F.lit("Info"))
        .when(et == "context",       F.lit("Info"))
        .when(et == "engine_error",  F.lit("Error Crítico"))
    )

    contexto = (
        F.when(et == "navigation",   F.col("msg_text"))
        .when(et == "integration",
            F.concat(F.lit("Se invocó a la integración "), F.coalesce(F.col("int_nombre"), F.lit("")))
        )
        .when(et == "variable",      F.col("msg_text"))
        .when(et == "context",       F.col("msg_text"))
        .when(et == "engine_error",  F.lit("Fallo interno de Yoizen"))
    )

    return (
        silver
        .select(
            tabla.alias("TABLA"),
            F.col("ts_utc").cast("string").alias("Timestamp"),
            F.col("fecha_real").alias("Fecha_Para_Relacion"),
            F.col("fecha_real").alias("Fecha_Visual"),
            F.col("case_id").alias("CaseId"),
            F.col("user_id").alias("UserId"),
            F.col("message_id").alias("MessageId"),
            bloque.alias("Bloque"),
            concepto.alias("Concepto"),
            valor_detalle.alias("Valor_Detalle"),
            estado.alias("Estado"),
            F.col("intervalo_30min").alias("Intervalo"),
            contexto.alias("Contexto"),
            F.col("int_request_raw").alias("Request"),
            F.col("int_response_raw").alias("Response"),
            F.col("int_request_legible").alias("Request_Tooltip"),
            F.col("int_response_legible").alias("Response_Tooltip"),
            F.col("int_motivo_error").alias("Motivo_Error"),
            F.col("int_tipo_error").alias("Tipo_Error"),
            F.col("dia_real"),
        )
        .filter(F.col("TABLA").isNotNull())
    )
