"""
Job Spark Structured Streaming : consomme le topic Kafka "online-retail" et
maintient des agregats en direct dans Postgres.

Design (choix assume, documente dans le README) :
- Le "temps reel" ici est celui du micro-batch Spark (trigger toutes les
  TRIGGER_SECONDS secondes), pas le temps evenementiel simule du dataset :
  Spark Structured Streaming ne supporte pas les fonctions de fenetrage/rang
  (row_number) directement sur un DataFrame streaming, donc le classement
  "top produits" se fait avec foreachBatch sur le micro-batch (DataFrame
  statique), ce qui est fiable et simple.
- Une fenetre "streaming_top_produits" = un micro-batch. Avec le producteur
  qui compresse le temps (voir producer.py --speedup), un micro-batch de
  TRIGGER_SECONDS secondes reelles correspond a plusieurs heures simulees.

Metriques calculees, ecrites dans Postgres (schema.sql) :
- streaming_ca_cumule   : CA net cumule depuis le debut + nb de lignes traitees.
- streaming_ca_pays     : CA net cumule par pays.
- streaming_top_produits: top 10 produits (par CA) du micro-batch courant.

Usage:
    python streaming_job.py
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, from_json, row_number, sum as spark_sum, count as spark_count, min as spark_min, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

import psycopg2

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "online-retail"
TRIGGER_SECONDS = "10 seconds"
TOP_N = 10

PG_DSN = dict(host="localhost", port=5433, dbname="retail", user="retail", password="retail")

SCHEMA = StructType([
    StructField("invoice_no", StringType()),
    StructField("stock_code", StringType()),
    StructField("description", StringType()),
    StructField("quantity", IntegerType()),
    StructField("invoice_date", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("customer_id", LongType()),
    StructField("country", StringType()),
])


def get_conn():
    return psycopg2.connect(**PG_DSN)


def upsert_ca_cumule(conn, batch_ca: float, batch_count: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO streaming_ca_cumule (id, ca_total, nb_lignes, updated_at)
            VALUES (1, %s, %s, now())
            ON CONFLICT (id) DO UPDATE
                SET ca_total = streaming_ca_cumule.ca_total + EXCLUDED.ca_total,
                    nb_lignes = streaming_ca_cumule.nb_lignes + EXCLUDED.nb_lignes,
                    updated_at = now()
            """,
            (batch_ca, batch_count),
        )
    conn.commit()


def upsert_ca_pays(conn, rows) -> None:
    with conn.cursor() as cur:
        for pays, ca in rows:
            cur.execute(
                """
                INSERT INTO streaming_ca_pays (pays, ca_total, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (pays) DO UPDATE
                    SET ca_total = streaming_ca_pays.ca_total + EXCLUDED.ca_total,
                        updated_at = now()
                """,
                (pays, ca),
            )
    conn.commit()


def upsert_top_produits(conn, window_start, window_end, rows) -> None:
    with conn.cursor() as cur:
        for stock_code, description, quantite, ca in rows:
            cur.execute(
                """
                INSERT INTO streaming_top_produits
                    (window_start, window_end, stock_code, description, quantite_totale, ca_fenetre)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (window_start, stock_code) DO UPDATE
                    SET quantite_totale = EXCLUDED.quantite_totale,
                        ca_fenetre = EXCLUDED.ca_fenetre
                """,
                (window_start, window_end, stock_code, description, quantite, ca),
            )
    conn.commit()


def process_batch(batch_df, batch_id: int) -> None:
    batch_df = batch_df.cache()
    total = batch_df.count()
    if total == 0:
        batch_df.unpersist()
        return

    conn = get_conn()
    try:
        agg = batch_df.agg(
            spark_sum("revenue").alias("ca"),
            spark_count("*").alias("n"),
            spark_min("invoice_date").alias("min_ts"),
            spark_max("invoice_date").alias("max_ts"),
        ).collect()[0]

        upsert_ca_cumule(conn, float(agg["ca"] or 0.0), int(agg["n"] or 0))

        pays_rows = (
            batch_df.groupBy("country")
            .agg(spark_sum("revenue").alias("ca"))
            .collect()
        )
        upsert_ca_pays(conn, [(r["country"], float(r["ca"] or 0.0)) for r in pays_rows])

        produits = (
            batch_df.groupBy("stock_code", "description")
            .agg(spark_sum("quantity").alias("quantite_totale"), spark_sum("revenue").alias("ca_fenetre"))
        )
        ranked = produits.withColumn(
            "rang", row_number().over(Window.orderBy(col("ca_fenetre").desc()))
        ).filter(col("rang") <= TOP_N).collect()

        window_start = agg["min_ts"]
        window_end = agg["max_ts"]
        upsert_top_produits(
            conn,
            window_start,
            window_end,
            [(r["stock_code"], r["description"], float(r["quantite_totale"] or 0), float(r["ca_fenetre"] or 0)) for r in ranked],
        )

        print(f"[batch {batch_id}] {total} lignes | CA batch = {agg['ca']:.2f} | fenetre {window_start} -> {window_end}")
    finally:
        conn.close()
        batch_df.unpersist()


def main() -> None:
    spark = (
        SparkSession.builder.appName("online-retail-streaming")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("invoice_date", col("invoice_date").cast("timestamp"))
        .withColumn("revenue", col("quantity") * col("unit_price"))
    )

    query = (
        parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", "./checkpoint")
        .trigger(processingTime=TRIGGER_SECONDS)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
