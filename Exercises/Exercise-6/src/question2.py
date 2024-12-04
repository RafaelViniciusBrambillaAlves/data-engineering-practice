
from pyspark.sql.functions import col, to_date, coalesce, count


def question_2(dataframes: list, destination_path: str) -> None:
    """
    Questão 2

    Paramêtro:
    - dataframes: Lista com os DataFrames
    - destination_path: caminho para salvar o resultado em .csv
    """
    
    df1 = dataframes[0].withColumn("date", to_date(col("start_time")))
    df1 = df1.groupby("date").agg(count("start_time").alias("daily_trip_count_df1"))
    
    df2 = dataframes[1].withColumn("date", to_date(col("started_at")))
    df2 = df2.groupby("date").agg(count("started_at").alias("daily_trip_count_df2"))

    combined_df = df1.join(df2, on = "date", how = 'outer')

    combined_df = combined_df.withColumn(
        "daily_trip_count", 
        coalesce(col("daily_trip_count_df1"), col("daily_trip_count_df2"))
    ).select("date", "daily_trip_count")

    combined_df.write.mode("overwrite").option("header", "true").csv(f"{destination_path}/question2")

    print(f"Dataframe Questão 2 salvo como CSV")