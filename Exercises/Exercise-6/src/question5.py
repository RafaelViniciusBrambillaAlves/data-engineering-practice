from pyspark.sql.functions import avg, col

def question_5 (dataframes, destination_path):

    df1 = dataframes[0].groupBy("gender").agg(avg("tripduration").alias("avg_duration")).orderBy("avg_duration")
    df1 = df1.filter(col("gender") != "NULL")
    
    df1.write.mode("overwrite").option("header", "true").csv(f"{destination_path}/question5")

    print(f"Dataframe Questão 5 salvo como CSV")