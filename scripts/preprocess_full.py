# preprocess.py - Full dataset
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col

spark = SparkSession.builder.appName("wikilinks-prep").getOrCreate()

# chemins bucket
input_path = "gs://pagerank-marah-wiki/raw/wikilinks_lang=en.ttl.bz2"
output_path = "gs://pagerank-marah-wiki/preproc/wikilinks_full.parquet"

# lire le fichier bz2 en texte
lines = spark.read.text(input_path)

# extraire subject (src) et object (dst)
df_pairs = lines.select(
    regexp_extract(col("value"), r"^<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>", 1).alias("src"),
    regexp_extract(col("value"), r"^<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>", 3).alias("dst")
).filter((col("src") != "") & (col("dst") != ""))

# sauvegarder tout en parquet
df_pairs.write.mode("overwrite").parquet(output_path)