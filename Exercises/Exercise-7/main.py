from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import zipfile
from pyspark.sql import Window

def extract_csv_from_zip(data_path: str, output_path: str) -> str:
    """
    Extrair o arquivo csv da pasta .zip 

    Parâmetros:
    - data_path: Caminho para extrair o arquivo \n
    - output_path: Caminho para salvar o arquivo 

    Retorna:
    - Caminho com o arquivo .csv extraido \n
    - Nome do arquivo .csv extraido
    """

    for file in os.listdir(data_path):
        print(file)
        if file.endswith(".zip"):
            zip_path = os.path.join(data_path, file)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_name in zip_ref.namelist():
                    if file_name.endswith('.csv'):
                        zip_ref.extract(file_name, output_path)
                        return os.path.join(output_path, file_name), file_name
            raise FileNotFoundError("Nenhum arquivo encontrado no Zip.")
        
        
def processed_data(df: F.DataFrame, csv_name: str) -> F.DataFrame:
    """
    Processa os dados dataframes

    Parâmetros:
    - df: Dataframe a ser processado \n
    - csv_name: Nome do Arquivo 

    Retorna:
    Dataframe processado
    """

    # 1
    df = df.withColumn("source_file", F.lit(csv_name))

    # 2
    file_date_raw = '-'.join(csv_name.split('-')[2:5])
    df = df.withColumn("file_date", F.to_date(F.lit(file_date_raw), "yyyy-MM-dd"))

    # 3 
    df = df.withColumn(
        "brand", 
        F.when(F.col("model").contains(' '), F.split(F.col("model"), ' ').getItem(0))
            .otherwise(F.lit("unknown"))
        )
    
    # 4
    df2 = df.select("model", "capacity_bytes").dropDuplicates()
    window_spec = Window.orderBy(F.col("capacity_bytes").desc())
    df2 = df2.withColumn("storage_ranking", F.rank().over(window_spec))
    df = df.join(df2.select("model", "storage_ranking"), on = "model", how = "left")

    # 5
    df = df.withColumn("primary_key", F.sha2(F.concat_ws("||", *df.columns), 256))

    return df
    

def main():
    spark = SparkSession.builder.appName("Exercise7").enableHiveSupport().getOrCreate()

    csv_path, csv_name = extract_csv_from_zip('data', 'data')

    df = spark.read.csv(csv_path, header = True, inferSchema = True)

    df = processed_data(df, csv_name)
    
    output_path = "data/processed_csv"
    df.write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"Dataframe processado salvo em: {output_path }")    

if __name__ == "__main__":
    main()
