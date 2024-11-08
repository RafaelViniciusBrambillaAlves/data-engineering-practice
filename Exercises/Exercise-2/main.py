import requests
import time
import pandas as pd
import os


def create_dir(caminho: str) -> None:
    """
    Criação do diretório de destino caso ele não exista 

    Parâmetros:
    - caminho: Caminho do diretório a ser criado.
    """
    os.makedirs(caminho, exist_ok=True)

def web_scraping(link: str, date_to_find: str, path: str) -> None:
    """
    Realiza o web scraping para encontrar um arquivo CSV específico com base em uma data
    e baixa o arquivo para o diretório especificado.

    Parâmetros:
    - link: URL base para o scraping.
    - date_to_find: Data que será procurada no conteúdo HTML.
    - path: Diretório onde o arquivo será salvo.
    """
    response = requests.get(link)

    if response.status_code != 200:
        print("Falha na requisição!")
        return 
    
    html_content = response.text

    if date_to_find in html_content:
        
        start_index = html_content.find(date_to_find)
    
        start_td_index = html_content.rfind('<td><a href="', 0, start_index)
    
        end_td_index = html_content.find('.csv', start_td_index) + 4 
        
        # nome do arquivo
        file_name = html_content[start_td_index + 13:end_td_index]
        print(f"Arquivo encontrado: {file_name}")

        file_response = requests.get(link + file_name)

        if file_response.status_code == 200:

            file_path = os.path.join(path, file_name)

            with open(file_path, 'wb') as f:

                f.write(file_response.content)

            print(f"Arquivo {file_name} baixado com sucesso")

        else:
            print("Falha ao baixar o arquivo.")

    else:
        print("Data 2024-01-19 10:27 não encontrada.")

    
def load_dataframe(path):
    """
    Carrega um arquivo CSV em um DataFrame pandas e exibe a linha com o valor
    máximo de 'HourlyDryBulbTemperature'.

    Parâmetros:
    - path: Caminho do arquivo CSV a ser carregado.
    """
    if not os.path.exists(path):
        print(f"Arquivo {path} não encontrado.")
        return
   
    try:
        df = pd.read_csv(path)
        max_temp_row = df.loc[df['HourlyDryBulbTemperature'].idxmax()]
        print("Linha com a temperatura máxima:")
        print(max_temp_row)
    except Exception as e:
        print(f'Erro ao carregar o Dataframe: {e}')

def main():
    create_dir('downloads')

    web_scraping('https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/', '2024-01-19 10:27', 'downloads')

    load_dataframe('downloads/01023199999.csv')

    time.sleep(10)


if __name__ == "__main__":
    main()
