from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():

    spark = SparkSession.builder.appName("PageRank-RDD-Parquet").getOrCreate()
    sc = spark.sparkContext

    # -----------------------
    # CONFIG
    # -----------------------
    input_path = "gs://pagerank-marah-wiki/preproc/wikilinks_full.parquet"
    output_path = "gs://pagerank-marah-wiki/results/full_rdd_2w_results"
    top10_output = "gs://pagerank-marah-wiki/results/top10_full_rdd_2w"
    iterations = 4
    damping = 0.85
    num_part = 20

    # -----------------------
    # LOAD PARQUET AS RDD
    # -----------------------
    df = spark.read.parquet(input_path).select("src", "dst")

    edges_rdd = df.rdd \
        .map(lambda row: (row['src'], row['dst'])) \
        .partitionBy(num_part)

    links = edges_rdd.groupByKey(num_part).mapValues(list).cache()
    ranks = links.mapValues(lambda _: 1.0)

    # -----------------------
    # PAGE RANK ITERATIONS
    # -----------------------
    for i in range(iterations):

        contributions = links.join(ranks).flatMap(
            lambda kv: [(dst, kv[1][1] / len(kv[1][0])) for dst in kv[1][0]]
        )

        ranks = contributions.reduceByKey(
                    lambda a, b: a + b,
                    num_part                   # <--- TO AVOID SHUFFLE
                ) \
                .mapValues(lambda contrib: (1 - damping) + damping * contrib)

    # -----------------------------
    # SAVE RESULTS
    # -----------------------------
    df_ranks = ranks.toDF(["node", "rank"])
    df_ranks.write.mode("overwrite").parquet(output_path)

    top10 = df_ranks.orderBy(col("rank").desc()).limit(10)
    top10.write.mode("overwrite").parquet(top10_output)

    spark.stop()


if __name__ == "__main__":
    main()