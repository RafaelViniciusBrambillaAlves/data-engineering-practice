from pyspark.sql import Window
from pyspark.sql.functions import col, to_date, count, row_number

def question_4 (dataframes, destination_path):

    df1 = dataframes[0].withColumn("date", to_date(col("start_time")))
    df1 = df1.groupby("date", "to_station_name").agg(count("to_station_name").alias("count")).orderBy("date")

    rank1 = Window.partitionBy("date").orderBy(col("count").desc())
    rank_df1 = df1.withColumn("rank", row_number().over(rank1)).withColumnRenamed("to_station_name", "station_name")
    rank_df1 = rank_df1.where(col("rank") <= 3).select("date", "station_name")

    df2 = dataframes[1].withColumn("date", to_date(col("ended_at")))
    df2 = df2.groupby("date", "end_station_name").agg(count("end_station_name").alias("count")).orderBy("date")

    rank2 = Window.partitionBy("date").orderBy(col("count").desc())
    rank_df2 = df2.withColumn("rank", row_number().over(rank2)).withColumnRenamed("end_station_name", "station_name")
    rank_df2 = rank_df2.where(col("rank") <= 3).select("date", "station_name")

    combined_df = rank_df1.union(rank_df2)

    combined_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{destination_path}/question4")

    print(f"Dataframe Questão 4 salvo como CSV")