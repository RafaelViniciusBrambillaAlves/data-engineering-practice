from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, unix_timestamp, avg
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
        df = spark.read.format("csv").option("header", "true").load(file)
        dataframes.append(df)

    return dataframes

def question_1(dataframes, destination_path):

    df1 = dataframes[0]
    df2 = dataframes[1]

    df1 = df1.withColumn("trip_duration_minutes", (unix_timestamp("end_time") - unix_timestamp("start_time")) / 60)
    df1 = df1.withColumn("date", to_date(col("start_time")))
    avg_duration_per_day1 = df1.groupby("date").agg(avg("trip_duration_minutes").alias("avg_trip_duration"))

    
    df2 = df2.withColumn("trip_duration_minutes", (unix_timestamp("ended_at"), unix_timestamp("started_at")) / 60 )
    df2 = df2.withColumn("date", to_date(col("start_time")))
    avg_duration_per_day2 = df2.groupby("date").agg(avg("trip_duration_minutes").alias("avg_trip_duration"))

    avg_duration_per_day1.show()
    avg_duration_per_day2.show()

    combined_df = avg_duration_per_day1.join(avg_duration_per_day2, on = "date", how = "outer")

    combined_df.write.option("header", "true").csv(destination_path)


def main():
    zip_folder_path = "data"
    temp_folder = "/app/tem_csvs"
    reports_folder = "reports"
    reports_questions = "reports/questions"

    create_dir(reports_folder)

    spark = SparkSession.builder.appName("Exercise6").enableHiveSupport().getOrCreate()

    extract_and_copy_csv(zip_folder_path, temp_folder, reports_folder)

    dataframes = read_csv_files(reports_folder, spark)

    question_1(dataframes, reports_questions)
        
if __name__ == "__main__":
    main()
