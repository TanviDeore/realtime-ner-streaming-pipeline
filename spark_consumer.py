import os
import spacy
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, from_json, to_json, struct
from pyspark.sql.types import ArrayType, StringType, StructType 

spark = (
    SparkSession.builder
    .appName("NERPipeline")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_IN  = os.getenv("KAFKA_IN", "topic1")
TOPIC_OUT = os.getenv("KAFKA_OUT", "topic2")

CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/tmp/ner_ckpt_run2")

#Read from Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", "latest")
    .load()
)

schema = StructType().add("text", StringType())
parsed = (
    df.selectExpr("CAST(value AS STRING) AS raw_value")
      .select(from_json(col("raw_value"), schema).alias("data"))
      .select("data.text")
)

def _entity_udf():
    holder = {"nlp": None}
    def extract_entities(text):
        if not text:
            return []
        if holder["nlp"] is None:
            holder["nlp"] = spacy.load(
                "en_core_web_sm",
                disable=["tagger","parser","lemmatizer","attribute_ruler","textcat"]
            )
        doc = holder["nlp"](text)
        return [e.text.strip() for e in doc.ents if e.text and e.text.strip()]
    return udf(extract_entities, ArrayType(StringType()))

entity_udf = _entity_udf()

entities = parsed.withColumn("entities", entity_udf(col("text")))

counts = (
    entities
    .select(explode(col("entities")).alias("entity"))
    .groupBy("entity")
    .count()
    .withColumnRenamed("count", "total")
)

out_df = counts.select(
    to_json(struct(col("entity"), col("total"))).alias("value")
)

query = (
    out_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("topic", TOPIC_OUT)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("update")
    .trigger(processingTime="60 seconds")
    .start()
)

query.awaitTermination()
