from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, unix_timestamp, avg, coalesce, count, month, max
import os
import zipfile
import shutil
from src.question1 import question_1
from src.question2 import question_2
from src.question3 import question_3
from src.question4 import question_4
from src.question5 import question_5
from src.question6 import question_6

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
    question_2(dataframes, reports_questions)
    question_3(dataframes, reports_questions)
    question_4(dataframes, reports_questions)
    question_5(dataframes, reports_questions)
    question_6(dataframes, reports_questions)
        
if __name__ == "__main__":
    main()
