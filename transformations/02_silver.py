# Silver — Parse, classify, and extract fields from raw bronze log messages
# Single streaming table replaces the five-table MVP silver layer.
# Partitioned by dia_real + event_type to serve gold.flog_maestro efficiently.

from pyspark import pipelines as dp
from pyspark.sql import functions as F, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType,
)
import json
import re

# ── UDF return schemas ────────────────────────────────────────────────────────

_REQ_RES_SCHEMA = StructType([
    StructField("req_raw",      StringType()),
    StructField("res_raw",      StringType()),
    StructField("req_legible",  StringType()),
    StructField("res_legible",  StringType()),
])

_STATUS_SCHEMA = StructType([
    StructField("status",       StringType()),
    StructField("tipo_error",   StringType()),
    StructField("motivo_error", StringType()),
])

_VAR_SCHEMA = StructType([
    StructField("nombre",    StringType()),
    StructField("valor",     StringType()),
    StructField("es_valida", BooleanType()),
])

# ── UDFs ──────────────────────────────────────────────────────────────────────

@F.udf(returnType=_REQ_RES_SCHEMA)
def _udf_req_res(text):
    """Extract request/response payloads from integration log messages.
    Handles two message formats produced by yFlow Executor."""
    if not text:
        return ("", "", "", "")
    # Format 1: "... con datos {req} y respondió {res} en tiempos ..."
    if "con datos " in text and " y respondió " in text:
        idx_datos    = text.find("con datos ")
        idx_resp     = text.find(" y respondió ")
        idx_tiempos  = text.find(" en tiempos ")
        req = text[idx_datos + 10: idx_resp].strip()
        end = idx_tiempos if idx_tiempos != -1 else len(text)
        res = text[idx_resp + 13: end].strip()
        return (req, res, req, res)
    # Format 2: "El servicio => ... Para los datos => {req} Devuelve => {res}"
    if "Para los datos =>" in text and "Devuelve =>" in text:
        idx_datos    = text.find("Para los datos =>")
        idx_devuelve = text.find("Devuelve =>")
        req = text[idx_datos + 17: idx_devuelve].strip()
        res = text[idx_devuelve + 11:].strip()
        return (req, res, req, res)
    return ("", "", "", "")


@F.udf(returnType=_STATUS_SCHEMA)
def _udf_status(level, response_raw):
    """Determine integration status and error classification from response body."""
    if level and level.lower() == "error":
        return ("Error", "Error Interno Bot", "Nivel de Log marcado como Error en Yoizen")
    if not response_raw or response_raw.strip() in ("", "null"):
        return ("Error", "Timeout / Sin Respuesta", "La integración no devolvió respuesta (Posible Timeout)")
    try:
        data = json.loads(response_raw)
        if isinstance(data, dict):
            hs = None
            try:
                hs = int(data.get("httpStatus", ""))
            except (ValueError, TypeError):
                pass
            if hs:
                if 400 <= hs < 500:
                    return ("Error", "Error de Petición (4xx)", f"HTTP Status {hs}")
                if hs >= 500:
                    return ("Error", "Error de Servidor (5xx)", f"HTTP Status {hs}")
            rc = data.get("responseCode")
            if rc is not None:
                if isinstance(rc, int) and (rc < 200 or rc >= 300):
                    return ("Error", "Error de Lógica/Negocio", f"responseCode numérico interno: {rc}")
                if isinstance(rc, str) and rc.upper() not in ("OK", "200"):
                    return ("Error", "Error de Lógica/Negocio", f"responseCode texto interno: {rc}")
            err = data.get("error")
            if err is True or (isinstance(err, str) and err.lower() == "true"):
                return ("Error", "Error de Lógica/Negocio", "Campo 'error' es True")
            if isinstance(err, str) and len(err) > 0 and err.lower() not in ("null", "false"):
                return ("Error", "Error de Lógica/Negocio", f"Mensaje API: {err}")
            if data.get("isError") is True:
                return ("Error", "Error de Lógica/Negocio", "Campo 'isError' principal es True")
            inner = data.get("data")
            if isinstance(inner, dict) and isinstance(inner.get("error"), dict):
                if inner["error"].get("isError") is True:
                    return ("Error", "Error de Lógica/Negocio", "Campo anidado 'data.error.isError' es True")
    except (json.JSONDecodeError, AttributeError):
        return ("Error", "Error de Formato", "La respuesta no es un JSON válido")
    return ("OK", "Sin Error", "")


@F.udf(returnType=_VAR_SCHEMA)
def _udf_variable(msg):
    """Parse 'Se estableció el valor de la variable X...' pattern.
    Returns variable name, value, and whether the variable is relevant (not in ignore list)."""
    _IGNORAR = frozenset({
        "bodyLength", "accessToken", "responseCode", "httpStatus",
        "url", "method", "timeout", "headers", "claveDerivacion",
        "loop", "index", "counter", "i", "j", "length",
    })
    m = re.search(
        r"Se estableció el valor de la variable (.+?) al valor obtenido de la expresión (.*)",
        msg or "",
    )
    if m:
        nombre = m.group(1).strip()
        valor  = m.group(2).strip()
        return (nombre, valor, nombre not in _IGNORAR)
    return (None, None, False)


@F.udf(returnType=LongType())
def _udf_duration(text):
    """Extract the 'total' duration in ms from an integration log message."""
    if not text:
        return None
    m = re.search(r'"total":\s*(\d+)', text)
    return int(m.group(1)) if m else None


# ── Event classification helper ───────────────────────────────────────────────

def _classify_event(col_name):
    c = F.col(col_name)
    return (
        F.when(c.contains("Bloque actual:"),                                                          F.lit("navigation"))
        .when(c.contains("Se continuará la ejecución en el bloque"),                                  F.lit("navigation"))
        .when(c.contains("ya fue visitado en esta ejecución") & c.contains("Se detiene la ejecución"), F.lit("navigation"))
        .when(c.contains("Inicio logueo de nuevo caso"),                                              F.lit("navigation"))
        .when(c.contains("El caso es nuevo, iniciamos"),                                              F.lit("navigation"))
        .when(c.contains("Terminó el procesamiento del flujo"),                                       F.lit("navigation"))
        .when(c.contains("No se encontro ninguna opcion del menu que coincida"),                      F.lit("navigation"))
        .when(c.contains("Se invocó a la integración"),                                               F.lit("integration"))
        .when(c.contains("El servicio =>"),                                                           F.lit("integration"))
        .when(c.contains("Se estableció el valor de la variable"),                                    F.lit("variable"))
        .when(c.contains("hay que establecer el valor") & c.contains("opcionSeleccionada"),           F.lit("variable"))
        .when(c.contains("textoLimpio") & c.contains("Se estableció"),                               F.lit("variable"))
        .when(c.contains("Segmentación de la cuenta"),                                                F.lit("context"))
        .when(c.contains("produjo un error: SyntaxError"),                                            F.lit("engine_error"))
        .when(c.contains("Hubo un error al invocar a la API de integración"),                         F.lit("engine_error"))
        .when(c.contains("No se continuara la ejecucion del caso porque no hay una transicion definida"), F.lit("engine_error"))
    )


# ── Silver table ──────────────────────────────────────────────────────────────

@dp.table(
    name="silver.parsed_events",
    comment="Parsed and classified log events. Single table replaces the five-table MVP "
            "silver layer. Partitioned by dia_real + event_type.",
    partition_cols=["dia_real", "event_type"],
)
@dp.expect_all_or_drop({
    "has_case_id":    "case_id IS NOT NULL",
    "has_event_type": "event_type IS NOT NULL",
})
def parsed_events():
    df = dp.read_stream("bronze.logs_yflow_executor")

    # ── Parse inner message JSON ──────────────────────────────────────────────
    df = (df
        .withColumn("msg_text",   F.get_json_object(F.col("message"), "$.msg"))
        .withColumn("log_level",  F.get_json_object(F.col("message"), "$.level"))
        .withColumn("case_id",    F.get_json_object(F.col("message"), "$.payload.caseId"))
        .withColumn("user_id",    F.get_json_object(F.col("message"), "$.payload.userId"))
        .withColumn("message_id", F.get_json_object(F.col("message"), "$.payload.messageId"))
        .filter(F.col("case_id").isNotNull())
    )

    # ── Classify ──────────────────────────────────────────────────────────────
    df = (df
        .withColumn("event_type", _classify_event("msg_text"))
        .filter(F.col("event_type").isNotNull())
    )

    # ── Timestamps (UTC → America/Bogota) ─────────────────────────────────────
    df = (df
        .withColumn("_ts_inner",  F.get_json_object(F.col("message"), "$.time"))
        .withColumn("ts_utc",     F.col("_ts_inner").cast("timestamp"))
        .withColumn("_ts_bogota", F.from_utc_timestamp(F.col("ts_utc"), "America/Bogota"))
        .withColumn("fecha_real", F.date_format(F.col("_ts_bogota"), "yyyy-MM-dd HH:mm:ss.SSS"))
        .withColumn("dia_real",   F.date_format(F.col("_ts_bogota"), "yyyy-MM-dd"))
        .withColumn("intervalo_30min",
            F.concat(
                F.date_format(F.col("_ts_bogota"), "HH:"),
                F.when(F.minute(F.col("_ts_bogota")) < 30, F.lit("00")).otherwise(F.lit("30")),
            )
        )
    )

    # ── Navigation fields ─────────────────────────────────────────────────────
    is_nav = F.col("event_type") == "navigation"
    msg    = F.col("msg_text")

    df = df.withColumn("nav_evento",
        F.when(is_nav,
            F.when(msg.contains("Bloque actual:"),                                                          F.lit("Paso por Bloque"))
            .when(msg.contains("Se continuará la ejecución en el bloque"),                                  F.lit("Salto a Bloque"))
            .when(msg.contains("ya fue visitado en esta ejecución"),                                        F.lit("Bucle Detectado"))
            .when(msg.contains("Inicio logueo de nuevo caso") | msg.contains("El caso es nuevo"),           F.lit("Inicio de Caso"))
            .when(msg.contains("Terminó el procesamiento del flujo"),                                       F.lit("Fin de Flujo"))
            .when(msg.contains("No se encontro ninguna opcion del menu que coincida"),                      F.lit("Input Inválido (Fallback)"))
        )
    )

    df = df.withColumn("nav_bloque",
        F.when(is_nav & msg.contains("Bloque actual:"),
            F.trim(F.regexp_extract(msg, r"Bloque actual:\s*(.+)$", 1)))
        .when(is_nav & msg.contains("Se continuará la ejecución en el bloque"),
            F.trim(F.regexp_extract(msg, r"ejecución en el bloque\s*(.+)$", 1)))
        .when(is_nav & msg.contains("ya fue visitado en esta ejecución"),
            F.lit("El sistema detuvo la ejecución por seguridad"))
        .when(is_nav & (msg.contains("Inicio logueo de nuevo caso") | msg.contains("El caso es nuevo")),
            F.lit("Apertura de caso/sesión"))
        .when(is_nav & msg.contains("Terminó el procesamiento del flujo"),
            F.lit("Cierre normal del procesamiento"))
        .when(is_nav & msg.contains("No se encontro ninguna opcion"),
            F.lit("El usuario ingresó una opción no válida"))
    )

    # ── Integration fields ────────────────────────────────────────────────────
    is_int = F.col("event_type") == "integration"

    df = df.withColumn("int_nombre",
        F.when(is_int & msg.contains("Se invocó a la integración"),
            F.trim(F.regexp_extract(msg, r"integración\s+(.+?)\s+con datos", 1)))
        .when(is_int & msg.contains("El servicio =>"),
            F.trim(F.regexp_extract(msg, r"El servicio =>\s*(.+?)\s+Para los datos", 1)))
    )

    df = (df
        .withColumn("_rr", F.when(is_int, _udf_req_res(msg)))
        .withColumn("int_request_raw",      F.col("_rr.req_raw"))
        .withColumn("int_response_raw",     F.col("_rr.res_raw"))
        .withColumn("int_request_legible",  F.col("_rr.req_legible"))
        .withColumn("int_response_legible", F.col("_rr.res_legible"))
        .withColumn("int_duracion_ms",      F.when(is_int, _udf_duration(msg)))
    )

    df = (df
        .withColumn("_st", F.when(is_int, _udf_status(F.col("log_level"), F.col("int_response_raw"))))
        .withColumn("int_status",       F.col("_st.status"))
        .withColumn("int_tipo_error",   F.col("_st.tipo_error"))
        .withColumn("int_motivo_error", F.col("_st.motivo_error"))
    )

    # ── Variable events (bot/user messages + context variables) ──────────────
    is_var = F.col("event_type") == "variable"

    df = (df
        .withColumn("_vp", F.when(
            is_var & msg.contains("Se estableció el valor de la variable"),
            _udf_variable(msg),
        ))
        # Special patterns override the regex-parsed values
        .withColumn("var_nombre",
            F.when(is_var & msg.contains("opcionSeleccionada"), F.lit("opcionSeleccionada"))
            .when(is_var & msg.contains("textoLimpio"),         F.lit("textoLimpio"))
            .otherwise(F.col("_vp.nombre"))
        )
        .withColumn("var_valor",
            F.when(is_var & msg.contains("opcionSeleccionada"),
                F.trim(F.regexp_extract(msg, r"establecer el valor\s+(.+?)\s+a la variable", 1)))
            .when(is_var & msg.contains("textoLimpio"),
                F.trim(F.regexp_extract(msg, r"expresión\s*(.+)$", 1)))
            .otherwise(F.col("_vp.valor"))
        )
        .withColumn("var_emisor",
            F.when(is_var & msg.contains("opcionSeleccionada"),                                    F.lit("Usuario (Botón)"))
            .when(is_var & msg.contains("textoLimpio"),                                            F.lit("Usuario"))
            .when(
                is_var
                & F.col("_vp.nombre").isNotNull()
                & (F.col("_vp.nombre").startswith("texto") | (F.col("_vp.nombre") == F.lit("body"))),
                F.lit("Bot"),
            )
        )
        .withColumn("var_es_valida", F.coalesce(F.col("_vp.es_valida"), F.lit(False)))
    )

    # var_es_mensaje: True  → goes to excluded 3_Mensajes (bot/user text)
    # var_es_contexto: True → goes to 4_Contexto (named variables)
    df = (df
        .withColumn("var_es_mensaje",  is_var & F.col("var_emisor").isNotNull())
        .withColumn("var_es_contexto", is_var & F.col("var_es_valida") & F.col("var_emisor").isNull())
    )

    # ── Context fields (Segmentación de la cuenta) ────────────────────────────
    is_ctx = F.col("event_type") == "context"
    df = (df
        .withColumn("ctx_variable", F.when(is_ctx, F.lit("SegmentacionCliente")))
        .withColumn("ctx_valor",    F.when(is_ctx, F.trim(F.regexp_extract(msg, r"=>\s*(.+)$", 1))))
    )

    # ── Engine error fields ───────────────────────────────────────────────────
    is_err = F.col("event_type") == "engine_error"
    df = (df
        .withColumn("err_type",
            F.when(msg.contains("produjo un error: SyntaxError"),                       F.lit("SyntaxError (Regla Bot)"))
            .when(msg.contains("Hubo un error al invocar a la API de integración"),      F.lit("Fallo Crítico API (No conectó)"))
            .when(msg.contains("No se continuara la ejecucion del caso"),               F.lit("Flujo Roto (Sin transición)"))
        )
        .withColumn("err_detalle",
            F.when(is_err & msg.contains("No se continuara la ejecucion"),
                F.lit("El usuario se quedó estancado porque falta flecha de diseño"))
            .when(is_err, msg)
        )
    )

    return df.select(
        # Partition columns first
        "dia_real",
        "event_type",
        # Timestamps
        "ts_utc",
        "fecha_real",
        "intervalo_30min",
        # Identifiers
        "case_id",
        "user_id",
        "message_id",
        "log_level",
        "msg_text",
        # Navigation
        "nav_evento",
        "nav_bloque",
        # Integration
        "int_nombre",
        "int_duracion_ms",
        "int_status",
        "int_tipo_error",
        "int_motivo_error",
        "int_request_raw",
        "int_response_raw",
        "int_request_legible",
        "int_response_legible",
        # Variable (messages + context)
        "var_nombre",
        "var_valor",
        "var_emisor",
        "var_es_mensaje",
        "var_es_contexto",
        # Context (Segmentación)
        "ctx_variable",
        "ctx_valor",
        # Engine errors
        "err_type",
        "err_detalle",
        # Metadata
        "source_file",
        "ingestion_timestamp",
    )


# ── Block enrichment ──────────────────────────────────────────────────────────

from datetime import datetime, timezone
from pyspark.sql.streaming.stateful_processor import StatefulProcessor, StatefulProcessorHandle


_TTL_MS = 24 * 60 * 60 * 1000  # 24 hours in milliseconds


class _BlockTracker(StatefulProcessor):
    """Propagates the active navigation block to every event in a case session.

    Groups the stream by case_id, sorts each micro-batch by ts_utc, and carries
    the last seen nav_bloque forward so integration events inherit the correct
    block context at their exact timestamp — not just the latest known block.

    State per case_id:
      - last_block:       last navigation block seen (STRING)
      - last_activity_ms: processing time of the most recent batch (LONG)

    Expiry strategy: on every batch of activity, a new 24h processing-time timer
    is registered. On expiry, if the case has been idle for the full 24h the state
    is cleared; if a newer batch arrived in the meantime the timer is stale and
    ignored. This avoids timer deletion (which requires storing the old timestamp)
    at the cost of a small number of no-op expiry calls per case.
    """

    def init(self, handle: StatefulProcessorHandle) -> None:
        self._handle = handle
        # getValueState requires a StructType — scalar types are not accepted directly
        self._last_block       = handle.getValueState(
            "last_block",       StructType([StructField("v", StringType())])
        )
        self._last_activity_ms = handle.getValueState(
            "last_activity_ms", StructType([StructField("v", LongType())])
        )

    def handleInputRows(self, _key, rows, timerValues):
        _state = self._last_block.get()
        last_block = _state.v if _state is not None else "Desconocido"
        now_ms = timerValues.getCurrentProcessingTimeInMs()

        for row in sorted(
            rows,
            key=lambda r: r.ts_utc if r.ts_utc is not None
                          else datetime.min.replace(tzinfo=timezone.utc),
        ):
            if row.event_type == "navigation" and row.nav_bloque:
                last_block = row.nav_bloque
                self._last_block.update(Row(v=last_block))
            yield {**row.asDict(), "active_block": last_block}

        # Reset the 24h expiry window on each batch of activity
        self._last_activity_ms.update(Row(v=now_ms))
        self._handle.registerTimer(now_ms + _TTL_MS)

    def handleExpiredTimer(self, _key, timerValues, _expiredTimerInfo):
        _state = self._last_activity_ms.get()
        last_activity = _state.v if _state is not None else 0
        # Only clear if the case has truly been idle for the full TTL.
        # If a newer batch arrived after this timer was registered the gap
        # will be < _TTL_MS and the timer is stale — a later one will fire.
        if timerValues.getCurrentProcessingTimeInMs() - last_activity >= _TTL_MS:
            self._last_block.clear()
            self._last_activity_ms.clear()
        return iter([])

    def close(self) -> None:
        pass


@dp.table(
    name="silver.events_enriched",
    comment="parsed_events enriched with active_block via transformWithState. "
            "Block context is exact at each event's timestamp within the session. "
            "State expires after 24h of inactivity per case_id.",
    partition_cols=["dia_real", "event_type"],
)
def events_enriched():
    parsed = dp.read_stream("silver.parsed_events")
    output_schema = StructType(
        parsed.schema.fields + [StructField("active_block", StringType(), True)]
    )
    return (
        parsed
        .groupBy("case_id")
        .transformWithState(
            _BlockTracker(),
            outputMode="append",
            timeMode="processingTime",
            outputStructType=output_schema,
        )
    )
