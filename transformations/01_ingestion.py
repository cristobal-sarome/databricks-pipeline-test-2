# Bronze — Auto Loader ingestion from Azure Blob Storage
# Streaming table with Event Grid notifications

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timezone, timedelta

# ── Configuration ────────────────────────────────────────────────────────────
INPUT_PATH = "abfss://logs@yoizenftplanding.dfs.core.windows.net/"
FILTRO = "*yflow-claroco*"

# Fluent Bit envelope schema
schema_envelope = StructType([
    StructField("@timestamp", StringType(), True),
    StructField("time", StringType(), True),
    StructField("stream", StringType(), True),
    StructField("logtag", StringType(), True),
    StructField("message", StringType(), True),
    StructField("kubernetes", StructType([
        StructField("pod_name", StringType(), True),
        StructField("namespace_name", StringType(), True),
        StructField("container_image", StringType(), True),
        StructField("host", StringType(), True),
    ]), True),
])


@dp.table(
    name="bronze.logs_yflow_executor",
    comment="Raw logs from yFlow Executor ingested via Auto Loader with Event Grid",
    partition_cols=["Dia_Real"]
)
@dp.expect("valid_message", "message IS NOT NULL")
@dp.expect("has_source_file", "source_file IS NOT NULL")
def logs_yflow_executor():
    processing_time_limit = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.maxFilesPerTrigger", "200")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("pathGlobFilter", FILTRO)
        #.option("modifiedAfter", processing_time_limit) # Para pruebas, solo procesar las últimas horas
        .schema(schema_envelope)
        .load(INPUT_PATH)
        # Enrich with metadata and convert UTC → Colombia timezone
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("pod_name", F.col("kubernetes.pod_name"))
        .withColumn("k8s_image", F.col("kubernetes.container_image"))
        .withColumn("_time_inner", F.get_json_object(F.col("message"), "$.time"))
        .withColumn("_ts_utc", F.col("_time_inner").cast("timestamp"))
        .withColumn("Dia_Real",
            F.coalesce(
                F.date_format(
                    F.from_utc_timestamp(F.col("_ts_utc"), "America/Bogota"),
                    "yyyy-MM-dd"
                ),
                F.lit(None)
            ))
        .select(
            "message",
            "ingestion_timestamp",
            "source_file",
            "pod_name",
            "k8s_image",
            "Dia_Real"
        )
    )
