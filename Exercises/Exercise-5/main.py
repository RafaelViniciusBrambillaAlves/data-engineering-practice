import psycopg2
import time
import pandas as pd
import os

def infer_sql_type(dtype):
    """
    Inferir o tipo de dados para cada coluna do DataFrame.

    Parâmetros:
    dtype -- O tipo de dados da coluna do DataFrame
    
    Retorna:
    Uma string representando o tipo de dado SQL correspondente (INT, FLOAT, BOOLEAN, TIMESTAMP ou VARCHAR).
    """
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "VARCHAR(255)"

def generate_create_table(csv_file_path: str, table_name: str, dic_foreign_key) -> str:
    """
    Gerar a instrução SQL para criar a tabela com base no arquivo CSV.

    Parâmetros:
    csv_file_path -- Caminho para o arquivo CSV
    table_name -- Nome da tabela a ser criada
    dic_foreign_key -- Dicionário para armazenar as chaves estrangeiras
    
    Retorna:
    Uma string contendo o comando SQL para criação da tabela.

    """
    df = pd.read_csv(csv_file_path, nrows=10)

    sql = f"CREATE TABLE {table_name} (\n"

    columns = []
    foreign_columns = []
    id = True
    for col in df.columns:
        
        col_type = infer_sql_type(df[col].dtype)
        if '_' in col and col.split('_')[1] == 'id' and id:
            dic_foreign_key[table_name] = col
            columns.append(f" {col} {col_type} PRIMARY KEY")
            id = False
        else:
            if col.endswith('id'):
                f_table = (col.split('_')[0] + 's').replace(' ', '')
                
                foreign_columns.append(f"  FOREIGN KEY ({col}) REFERENCES {f_table}({dic_foreign_key[f_table]})")
            
            columns.append(f" {col} {col_type}")

    if foreign_columns == []:
        sql += ",\n".join(columns) + "\n);"
    else:
        sql += ",\n".join(columns) +",\n" +",\n".join(foreign_columns) + "\n);"
    return sql

def generate_insert_data(csv_file_path: str, table_name: str) -> str:
    """
    Gerar a instrução SQL para inserir os dados na tabela com base no arquivo CSV.

    Parâmetros:
    csv_file_path -- Caminho para o arquivo CSV
    table_name -- Nome da tabela para inserção dos dados
    
    Retorna:
    A instrução SQL de inserção e os dados a serem inseridos.

    """
    df = pd.read_csv(csv_file_path)
    for _, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        columns = ', '.join(row.index)
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        data = [tuple(row) for row in df.values]

    return sql, data

def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    # your code here
    cur = conn.cursor()

    dic_foreign_key = {}

    for root, dirs, files in os.walk('data'):
        for file in files:
            print(file)
            if file.endswith('.csv'):

                csv_file_path = 'data/' + file
                table_name = file.split('.')[0]

                

                create_table_sql  = generate_create_table(csv_file_path, table_name, dic_foreign_key)
                # print(create_table_sql)

                try:
                    cur.execute(create_table_sql)
                    conn.commit()
                    print(f"A tabela {table_name} foi criada com sucesso")
                except Exception as e:
                    print(f"Erro ao criar a tabela {table_name}: {e}")
                    conn.rollback()
                
                try:
                    sql, data = generate_insert_data(csv_file_path, table_name)
                    cur.executemany(sql, data)
                    conn.commit()
                    print(f'Dados da tabela {table_name}, foram inseridos com sucesso')
                except Exception as e:
                    print(f'Erro ao inserir dados da tabela {table_name}: {e}')
                    conn.rollback()

                print()
                
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
