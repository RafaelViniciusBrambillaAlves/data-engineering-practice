from pyspark.sql.functions import avg, col, current_date, year, round
import datetime


def question_6 (dataframes, destination_path):

    df1 = dataframes[0].withColumn("age", year(current_date()) - col("birthyear"))

    df_avg_duration = df1.groupBy("age").agg(round(avg("tripduration"), 2).alias("avg_tripduration")).orderBy("avg_tripduration")
    df_avg_duration = df_avg_duration.filter(col("age").isNotNull() & col("avg_tripduration").isNotNull())

    df_shortest_avg_tripduration = df_avg_duration.limit(10)
    df_longest_avg_tripduration = df_avg_duration.orderBy(col("avg_tripduration").desc()).limit(10)

    df_shortest_avg_tripduration.write.mode("overwrite").option("header", "true").csv(f"{destination_path}/question6/shortest_avg_tripduration")
    df_longest_avg_tripduration.write.mode("overwrite").option("header", "true").csv(f"{destination_path}/question6/longest_avg_tripduration")

    print(f"Dataframes Questão 6 salvo como CSV")