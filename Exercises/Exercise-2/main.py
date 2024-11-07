import requests
import time
# import pandas
import os


def create_dir(caminho: str) -> None:
    '''
    Criação do diretório de destino caso ele não exista 

    Parâmetros:
    - caminho: Caminho do diretório a ser criado.
    '''
    os.makedirs(caminho, exist_ok=True)

def web_scraping(link, date_to_find):
    response = requests.get(link)

    if response.status_code != 200:
        print("Falha na requisição!")
        return 
    
    html_content = response.text

    if date_to_find in html_content:
        
        start_index = html_content.find(date_to_find)
        print(start_index)
        start_td_index = html_content.rfind('<td><a href="', 0, start_index)
        print(start_td_index)
        end_td_index = html_content.find('.csv', start_td_index) + 4 
        
        # Extraindo o nome do arquivo
        file_name = html_content[start_td_index + 14:end_td_index]
        print(f"Arquivo encontrado: {file_name}")

    else:
        print("Data 2024-01-19 10:27 não encontrada.")

        


def main():
    create_dir('downloads')
    web_scraping('https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/', '2024-01-19 10:27')
    print('boaaaaaaaa')


    time.sleep(10)


if __name__ == "__main__":
    main()
