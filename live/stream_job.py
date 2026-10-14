"""Record-level Spark Structured Streaming job for live fault-injection trials.

Unlike src/pipeline.py (windowed counts), this job keeps every record's
sequence number, so the trial runner can count duplicated and missing records
after a failure. It runs until the runner creates a stop file, then stops the
query gracefully.

Source:  kafka (measured runs) or files (offline tests of the runner only)
Sink:    file         Spark's built-in JSON file sink: exactly-once output via
                      the _spark_metadata commit log
         foreachbatch every micro-batch appended as a new plain file:
                      at-least-once (a replayed batch is written again)
"""
import argparse
import json
import os
import sys
import time
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StringType, StructField, StructType

SCHEMA = StructType([
    StructField("seq", LongType(), True),
    StructField("event_id", StringType(), True),
])


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["kafka", "files"], default="kafka")
    p.add_argument("--broker", default="localhost:9092")
    p.add_argument("--topic")
    p.add_argument("--input-dir", help="files source only")
    p.add_argument("--checkpoint", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--trigger", required=True, help='e.g. "1 second"')
    p.add_argument("--sink", choices=["file", "foreachbatch"], required=True)
    p.add_argument("--stop-file", required=True)
    a = p.parse_args()

    import pyspark
    builder = (SparkSession.builder.appName("live-ft-trial").master("local[2]")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false"))
    if a.source == "kafka":
        builder = builder.config(
            "spark.jars.packages",
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if a.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", a.broker)
               .option("subscribe", a.topic)
               .option("startingOffsets", "earliest")
               .option("failOnDataLoss", "true")
               .load())
        rows = (raw.select(from_json(col("value").cast("string"), SCHEMA).alias("d"),
                           col("partition").alias("kpartition"), col("offset").alias("koffset"))
                .select("d.seq", "d.event_id", "kpartition", "koffset"))
    else:
        rows = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 50).json(a.input_dir)

    writer = rows.writeStream.option("checkpointLocation", a.checkpoint).trigger(
        processingTime=a.trigger)
    if a.sink == "file":
        q = writer.format("json").option("path", a.output).outputMode("append").start()
    else:
        os.makedirs(a.output, exist_ok=True)

        def write_batch(df, batch_id):
            recs = [r.asDict() for r in df.collect()]
            path = os.path.join(a.output, f"batch-{batch_id:06d}-{uuid.uuid4().hex[:8]}.jsonl")
            with open(path, "w", encoding="utf-8") as f:
                for r in recs:
                    f.write(json.dumps(r) + "\n")

        q = writer.foreachBatch(write_batch).start()

    print(f"STREAM_STARTED {time.time():.6f}", flush=True)
    while q.isActive and not os.path.exists(a.stop_file):
        time.sleep(0.5)
    if q.isActive:
        q.stop()
    print(f"STREAM_STOPPED {time.time():.6f}", flush=True)
    spark.stop()
    if q.exception() is not None:
        print(f"QUERY_EXCEPTION {q.exception()}", flush=True)
        sys.exit(3)


if __name__ == "__main__":
    main()
