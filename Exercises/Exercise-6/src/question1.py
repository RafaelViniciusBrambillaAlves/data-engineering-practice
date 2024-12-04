from pyspark.sql.functions import col, to_date, unix_timestamp, avg, round, coalesce

def question_1(dataframes: list, destination_path: str) -> None:
    """
    Questão 1

    Paramêtro:
    - dataframes: Lista com os DataFrames
    - destination_path: caminho para salvar o resultado em .csv
    """

    df1 = dataframes[0].withColumn("trip_duration_minutes", (unix_timestamp("end_time") - unix_timestamp("start_time")) / 60)
    df1 = df1.withColumn("date", to_date(col("start_time")))
    avg_duration_per_day1 = df1.groupby("date").agg(round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_df1"))

    
    df2 = dataframes[1].withColumn("trip_duration_minutes", (unix_timestamp("ended_at") - unix_timestamp("started_at")) / 60 )
    df2 = df2.withColumn("date", to_date(col("started_at")))
    avg_duration_per_day2 = df2.groupby("date").agg(round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_df2"))

    combined_df = avg_duration_per_day1.join(avg_duration_per_day2, on = "date", how = "outer")
    
    combined_df = combined_df.withColumn(
        "avg_trip_duration",
        coalesce(col("avg_trip_duration_df1"), col("avg_trip_duration_df2"))
    ).select("date", "avg_trip_duration")

    combined_df.write.mode("overwrite").option("header", "true").csv(f"{destination_path}/question1")
 
    print(f"Dataframe Questão 1 salvo como CSV")