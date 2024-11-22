from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, unix_timestamp, avg, coalesce, count, month, max
import os
import zipfile
import shutil

def create_dir(path: str) -> None:
    """
    Criação do diretório de destino caso ele não exista 

    Parâmetros:
    - path: Caminho do diretório a ser criado.
    """

    os.makedirs(path, exist_ok=True)

def extract_and_copy_csv(zip_folder_path, temp_folder, reports_folder):
    
    create_dir(temp_folder)
    create_dir(reports_folder)

    zip_files = [
        os.path.join(zip_folder_path, file)
        for file in os.listdir(zip_folder_path) if file.endswith(".zip")
    ]

    for zip_file in zip_files:
        print(f'Processando arquivo: {zip_file}')

        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(temp_folder)

        for file in os.listdir(temp_folder):
            if file.endswith(".csv"):
                csv_path = os.path.join(temp_folder, file)
                destination_path = os.path.join(reports_folder, file)
                print(f"Copiando {csv_path} para {destination_path}")
                shutil.copy(csv_path, destination_path)

    shutil.rmtree(temp_folder)

def read_csv_files(reports_folder, spark):
    
    csv_files = [
        os.path.join(reports_folder, file)
        for file in os.listdir(reports_folder) if file.endswith(".csv")
    ]

    dataframes = []

    for file in csv_files:
        print(f"Lendo CSV: {file}")
        df = spark.read.csv(file, header=True, inferSchema=True)
        dataframes.append(df)

    return dataframes

def question_1(dataframes, destination_path):

    df1 = dataframes[0].withColumn("trip_duration_minutes", (unix_timestamp("end_time") - unix_timestamp("start_time")) / 60)
    df1 = df1.withColumn("date", to_date(col("start_time")))
    avg_duration_per_day1 = df1.groupby("date").agg(avg("trip_duration_minutes").alias("avg_trip_duration_df1"))

    
    df2 = dataframes[1].withColumn("trip_duration_minutes", (unix_timestamp("ended_at") - unix_timestamp("started_at")) / 60 )
    df2 = df2.withColumn("date", to_date(col("started_at")))
    avg_duration_per_day2 = df2.groupby("date").agg(avg("trip_duration_minutes").alias("avg_trip_duration_df2"))

    combined_df = avg_duration_per_day1.join(avg_duration_per_day2, on = "date", how = "outer")
    
    combined_df = combined_df.withColumn(
        "avg_trip_duration",
        coalesce(col("avg_trip_duration_df1"), col("avg_trip_duration_df2"))
    ).select("date", "avg_trip_duration")

    combined_df.write.mode("overwrite").option("header", "true").csv(destination_path)

    combined_df.show()

    print(f"Dataframe Questão 1 salvo como um único arquivo CSV em {destination_path}")


def question_2(dataframes, destination_path):
    
    df1 = dataframes[0].withColumn("date", to_date(col("start_time")))
    df1 = df1.groupby("date").agg(count("start_time").alias("daily_trip_count_df1"))
    
    df2 = dataframes[1].withColumn("date", to_date(col("started_at")))
    df2 = df2.groupby("date").agg(count("started_at").alias("daily_trip_count_df2"))

    combined_df = df1.join(df2, on = "date", how = 'outer')

    combined_df = combined_df.withColumn(
        "daily_trip_count", 
        coalesce(col("daily_trip_count_df1"), col("daily_trip_count_df2"))
    ).select("date", "daily_trip_count")

    combined_df.write.mode("overwrite").option("header", "true").csv(destination_path)

    combined_df.show()

    print(f"Dataframe Questão 2 salvo como um único arquivo CSV em {destination_path}")


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
    
    df1_max_station.show()
    df2_max_station.show()

    combined_df = df1_max_station.union(df2_max_station).orderBy("month_max").withColumnRenamed("month_max", "month")
    combined_df.show()



def main():
    zip_folder_path = "data"
    temp_folder = "/app/tem_csvs"
    reports_folder = "reports"
    reports_questions = "reports/questions"

    create_dir(reports_folder)

    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    # extract_and_copy_csv(zip_folder_path, temp_folder, reports_folder)

    dataframes = read_csv_files(reports_folder, spark)

    # question_1(dataframes, reports_questions)
    # question_2(dataframes, reports_questions)
    question_3(dataframes, reports_folder)
        
if __name__ == "__main__":
    main()
