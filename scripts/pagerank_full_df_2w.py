    
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sum as spark_sum

def main():
    spark = SparkSession.builder.appName("PageRank-DataFrame-Parquet").getOrCreate()

    # -----------------------------
    # CONFIG
    # -----------------------------
    input_path = "gs://pagerank-marah-wiki/preproc/wikilinks_full.parquet"   
    output_path = "gs://pagerank-marah-wiki/results/df_full_2w_results"
    top10_output = "gs://pagerank-marah-wiki/results/top10_full_df_2w"
    iterations = 4
    damping = 0.85

    # Stable partition count after testing on sample
    num_partitions = 4     


    # -----------------------------
    # LOAD PARQUET
    # -----------------------------
    edges = (
    spark.read.parquet(input_path)
         .select("src", "dst")
         .repartition(num_partitions, "src")
         .cache()
)



    # -----------------------------
    # BUILD NODE LIST
    # -----------------------------
    nodes = edges.select("src").union(edges.select("dst")).distinct().cache()


    # -----------------------------
    # OUTDEGREE
    # -----------------------------
    outdeg = (
    edges.groupBy("src")
         .count()
         .withColumnRenamed("count", "outdeg")
         .cache()
)

    # -----------------------------
    # INITIAL RANKS
    # -----------------------------
    ranks = nodes.withColumn("rank", lit(1.0))


    # -----------------------------
    # PAGE RANK ITERATIONS
    # -----------------------------
    for i in range(iterations):
        print(f"--- Iteration {i+1} ---")

        ranked = ranks.join(outdeg, "src", "left")

        contribs = ranked.withColumn(
            "contrib", col("rank") / col("outdeg")
        )

        propagated = (
            edges.join(contribs, "src", "left")
                .groupBy("dst")
                .agg(spark_sum("contrib").alias("sum_contrib"))
        )

        ranks = (
            nodes.join(propagated, nodes["src"] == propagated["dst"], "left")
                .select(
                    nodes["src"].alias("src"),
                    ((1 - damping) + damping * col("sum_contrib")).alias("rank")
                )
                .fillna({"rank": 1.0 - damping})
        )


    # -----------------------------
    # SAVE RESULTS
    # -----------------------------
    ranks.write.mode("overwrite").parquet(output_path)

    ranks.orderBy(col("rank").desc()).limit(10) \
         .write.mode("overwrite").parquet(top10_output)

    print("âœ” PageRank terminÃ© et sauvegardÃ©.")


if __name__ == "__main__":
    main()
