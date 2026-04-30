# Gold — Unified fact table fLog_Maestro
#
# Streaming table reading from silver.events_enriched, which already carries
# the correct active_block context per event (resolved by transformWithState).
# No window functions, no full-table scans — fully incremental.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="gold.flog_maestro",
    comment="Unified fact table consumed by BI/dashboards. Maps all event types to a "
            "shared schema. Block context is exact per event, resolved in silver.",
    partition_cols=["dia_real"],
)
@dp.expect("has_case_id", "CaseId IS NOT NULL")
def flog_maestro():
    enriched = dp.read_stream("silver.events_enriched")

    # ── Exclude events not consumed by the business ───────────────────────────
    # var_es_mensaje  → 3_Mensajes category, excluded per spec
    # variable events that are neither messages nor valid context variables
    # (i.e. names in NOMBRES_IGNORAR like bodyLength, accessToken, etc.)
    et = F.col("event_type")
    enriched = enriched.filter(
        ~F.col("var_es_mensaje")
        & ((et != "variable") | F.col("var_es_contexto"))
    )

    # ── Shared schema column expressions ──────────────────────────────────────

    tabla = (
        F.when(et == "navigation",  F.lit("1_Navegacion"))
        .when(et == "integration",  F.lit("2_Integraciones"))
        .when(et == "variable",     F.lit("4_Contexto"))
        .when(et == "context",      F.lit("4_Contexto"))
        .when(et == "engine_error", F.lit("5_Errores_Motor"))
    )

    bloque = (
        F.when(et == "navigation",  F.col("nav_bloque"))
        .when(et == "integration",  F.col("active_block"))
        .when(et == "engine_error", F.lit("Error de Sistema"))
        .otherwise(F.lit("Sin Registro de Bloque"))
    )

    concepto = (
        F.when(et == "navigation",  F.col("nav_evento"))
        .when(et == "integration",  F.col("int_nombre"))
        .when(et == "variable",     F.col("var_nombre"))
        .when(et == "context",      F.col("ctx_variable"))
        .when(et == "engine_error", F.col("err_type"))
    )

    valor_detalle = (
        F.when(et == "navigation",  F.col("nav_bloque"))
        .when(et == "integration",
            F.concat(
                F.coalesce(F.col("int_duracion_ms").cast("string"), F.lit("0")),
                F.lit(" ms"),
            )
        )
        .when(et == "variable",     F.col("var_valor"))
        .when(et == "context",      F.col("ctx_valor"))
        .when(et == "engine_error", F.col("err_detalle"))
    )

    estado = (
        F.when(et == "navigation",  F.col("log_level"))
        .when(et == "integration",  F.col("int_status"))
        .when(et == "variable",     F.lit("Info"))
        .when(et == "context",      F.lit("Info"))
        .when(et == "engine_error", F.lit("Error Crítico"))
    )

    contexto = (
        F.when(et == "navigation",  F.col("msg_text"))
        .when(et == "integration",
            F.concat(F.lit("Se invocó a la integración "), F.coalesce(F.col("int_nombre"), F.lit("")))
        )
        .when(et == "variable",     F.col("msg_text"))
        .when(et == "context",      F.col("msg_text"))
        .when(et == "engine_error", F.lit("Fallo interno de Yoizen"))
    )

    return (
        enriched
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
