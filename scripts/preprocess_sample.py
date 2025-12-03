# preprocess.py - DataFrame approach
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col

spark = SparkSession.builder.appName("wikilinks-prep").getOrCreate()

# chemins bucket
input_path = "gs://pagerank-marah-wiki/raw/wikilinks_lang=en.ttl.bz2"
output_path = "gs://pagerank-marah-wiki/preproc/sample_10pct.parquet"

# lire le fichier bz2 en texte
lines = spark.read.text(input_path)

# extraire subject (src) et object (dst)
df_pairs = lines.select(
    regexp_extract(col("value"), r"^<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>", 1).alias("src"),
    regexp_extract(col("value"), r"^<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>", 3).alias("dst")
).filter((col("src") != "") & (col("dst") != ""))

# sample 10% pour tests
sampled = df_pairs.sample(fraction=0.10, seed=42)

# sauvegarder en parquet
sampled.write.mode("overwrite").parquet(output_path)