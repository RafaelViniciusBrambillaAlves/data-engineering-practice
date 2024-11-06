import requests
import os
import zipfile
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def download_files(url: str, dest_path: str) -> None:
    '''
    Função que realiza o download de um arquivo ZIP, extrai seu conteúdo
    e organiza os arquivos extraídos no diretório de destino especificado.

    Parâmetros:
    - url: Endereço do arquivo ZIP a ser baixado.
    - dest_path: Caminho onde os arquivos extraídos serão salvos.
    '''
    
    if requests.head(url, timeout = 5).status_code == 200:

        dest_path = dest_path + '/' + url.split('/')[-1].split('.')[0]

        print(dest_path)

        response = requests.get(url, stream=True)

        with zipfile.ZipFile(BytesIO(response.content)) as z:

            for file in z.namelist():
                
                if file.endswith('/'):
                    continue 

                final_path = os.path.join(dest_path, file)

                os.makedirs(os.path.dirname(final_path), exist_ok=True)

                with z.open(file) as zip_file:

                    with open(final_path, 'wb') as final_file:
                        final_file.write(zip_file.read()) 

                    print('Arquivo salvo com sucesso')
    else:
        print('Link do arquivo inávlido')

def create_dir(caminho: str) -> None:
    '''
    Criação do diretório de destino caso ele não exista 

    Parâmetros:
    - caminho: Caminho do diretório a ser criado.
    '''
    os.makedirs(caminho, exist_ok=True)

def dowload_files_parallel(urls: str, dest_path: str) -> None:
    '''
    Função para realizar o download paralelo dos arquivos de uma lista de URLs.

    Parâmetros:
    - urls: Lista de URLs de arquivos ZIP a serem baixados.
    - dest_path: Caminho onde os arquivos extraídos serão salvos.
    '''
    with ThreadPoolExecutor(max_workers = 5) as executor:
        executor.map(lambda url: download_files(url, dest_path), urls)

def main():
    '''
    Função principal que gerencia o fluxo do programa.
    '''
    create_dir('downloads')
    dowload_files_parallel(download_uris, 'downloads')
    
if __name__ == "__main__":
    main()
