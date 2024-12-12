import duckdb
import pandas as pd

def create_insert_data(csv_file, table_name, conn):

    df = pd.read_csv(csv_file)

    type_mapping = {
        'object': 'TEXT',
        'int64': 'BIGINT',
        'float64': 'DOUBLE'
    }
    
    ddl = f"""CREATE TABLE IF NOT EXISTS {table_name} ( """

    for col_name, col_dtype in df.dtypes.items():
        if not col_name.isidentifier():
            col_name = f'"{col_name}"'
        col_name = col_name.lower().replace(' ', '_').replace('-', '_')
        col_dtype = type_mapping.get(str(col_dtype), 'TEXT')
        ddl += f"\n {col_name} {col_dtype}," 

    ddl = list(ddl)
    ddl.pop(len(ddl)- 1)
    ddl = ''.join(ddl)
    ddl += "\n);"

    try:
        conn.execute(ddl)
        print("Tabela criada com sucesso")
    except Exception as e:
        print("Erro ao criar a tabela:", e)
        return

    try:
        conn.execute(f"""
            COPY electric_cars FROM '{csv_file}' (AUTO_DETECT TRUE);
        """)
        print("Dados carregados com sucesso")
    except Exception as e:
        print("Erro ao carregar os dados:", e)
        return
    
def questions(conn):

    print("Contagem dos números de carros por cidade")
    results = conn.execute("""
            SELECT city AS cidade, 
                   COUNT(city) AS quantidade 
            FROM electric_cars 
            GROUP BY city;
        """).fetchall()
    for row in results:
        print(row)


    print("Top 3 carros elétricos mais populares")
    results = conn.execute("""
            SELECT make AS marca,
                   model AS modelo,
                   COUNT(model) AS popularidade
            FROM electric_cars  
            GROUP BY make, model
            ORDER BY popularidade DESC
            LIMIT 3;         
        """).fetchall()
    for row in results:
        print(row)

    print("Carro mais popular em cada código postal")
    results = conn.execute("""
            SELECT codigo_postal,
                   marca,
                   modelo 
            FROM (
                SELECT "Postal_Code" AS codigo_postal,
                    make AS marca,
                    model AS modelo,
                    COUNT(model) AS popularidade,
                    ROW_NUMBER() OVER (PARTITION BY "Postal_Code" ORDER BY COUNT(model) DESC) AS rank
                FROM electric_cars
                GROUP BY "Postal_Code", make, model
            )
            WHERE rank = 1
            ORDER BY codigo_postal;
        """).fetchall()
    for row in results:
        print(row)
    
    print("Quantidade de carros por ano de modelo")
    results = conn.execute("""
            SELECT Model_Year AS ano_modelo,
                   COUNT(Model) AS quantidade
            FROM electric_cars
            GROUP BY Model_Year
            ORDER BY Model_Year ASC
        """).fetchdf()
    
    output_dir = "data/electric_cars_by_year"
    results.to_parquet(output_dir, index = False, partition_cols = ["ano_modelo"])

    
def main():

    csv_file = "data/Electric_Vehicle_Population_Data.csv"
    table_name = "electric_cars"
    conn = duckdb.connect(database='car_database.duckdb', read_only=False)

    create_insert_data(csv_file, table_name, conn)

    questions(conn)


if __name__ == "__main__":
    main()
