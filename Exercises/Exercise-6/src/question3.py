
from pyspark.sql.functions import count, month, max

def question_3(dataframes, destination_path):

    df1 = dataframes[0].withColumn("month", month("start_time"))
    df1 = df1.groupby("from_station_name", "month").agg(count("from_station_name").alias("count"))

    df1_max_station = df1.groupby("month").agg(max("count").alias("max_count")).withColumnRenamed("month", "month_max")
    df1_max_station = df1_max_station.join(
        df1, 
        (df1_max_station["month_max"] == df1["month"]) & (df1_max_station["max_count"] == df1["count"])
    ).select(
        "month_max", "from_station_name"
    )

    df2 = dataframes[1].withColumn("month", month("started_at"))
    df2 = df2.groupby("start_station_name", "month").agg(count("start_station_name").alias("count"))

    df2_max_station = df2.groupby("month").agg(max("count").alias("max_count")).withColumnRenamed("month", "month_max")
    df2_max_station = df2_max_station.join(
        df2,
        (df2_max_station["month_max"] == df2["month"]) & (df2_max_station["max_count"] == df2["count"])
    ).select(
        "month_max", "start_station_name"
    )
    
    combined_df = df1_max_station.union(df2_max_station).orderBy("month_max").withColumnRenamed("month_max", "month")

    combined_df.write.mode("overwrite").option("header", "true").csv(f"{destination_path}/question3")

    print(f"Dataframe Questão 3 salvo como CSV")