from pyspark.sql.functions import avg, col, round

def question_5 (dataframes: list, destination_path: str) -> None:
    """
    Questão 5

    Paramêtro:
    - dataframes: Lista com os DataFrames
    - destination_path: caminho para salvar o resultado em .csv
    """

    df1 = dataframes[0].groupBy("gender").agg(round(avg("tripduration"), 2).alias("avg_duration")).orderBy("avg_duration")
    df1 = df1.filter(col("gender") != "NULL")
    
    df1.write.mode("overwrite").option("header", "true").csv(f"{destination_path}/question5")

    print(f"Dataframe Questão 5 salvo como CSV")