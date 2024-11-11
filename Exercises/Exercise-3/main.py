import requests
import io
import gzip
import time
import os
import boto3

def create_dir(caminho: str) -> None:
    """
    Criação do diretório de destino caso ele não exista 

    Parâmetros:
    - caminho: Caminho do diretório a ser criado.
    """
    os.makedirs(caminho, exist_ok=True)

def download_gz_file(bucket_name, key):
    """
    Faz o download de um arquivo .gz e o descomprime.

    Parâmetros:
    - bucket_name: Nome do bucket S3.
    - key: Caminho do arquivo no bucket.

    Retorna:
    - Conteúdo descompactado do arquivo como string, ou None em caso de erro.
    """

    # Usando a URL pública para acessar o arquivo via HTTP
    url = f"https://data.commoncrawl.org/{key}"

    # Fazendo o download do arquivo usando requests
    response = requests.get(url)
    
    # Verificando se a requisição foi bem-sucedida
    if response.status_code == 200:
        gz_buffer = io.BytesIO(response.content)
        
        try:
            # Descomprime o conteúdo do arquivo .gz e o lê como texto
            with gzip.open(gz_buffer, 'rt') as f:
                content = f.read()
            
            return content
        except gzip.BadGzipFile:
            print("Erro ao descompactar o arquivo")
            return None
    else:
        print(f"Erro ao baixar o arquivo: {response.status_code}")
        return None

def extract_first_uri(content):

    """
    Extrai o URI da primeira linha de um conteúdo de texto.

    Parâmetros:
    - content: Conteúdo do arquivo como string.

    Retorna:
    - URI da primeira linha ou None se o conteúdo estiver vazio.
    """
    
    if content:
        lines = content.splitlines()
        if lines:
            return lines[0]
        else:
            print("O conteudo está vazio")
            return None 
    else:
        print('Nenhum conteudo para extrair')
        return None
    
def download_file_from_s3(uri, bucket_name):
    """
    Faz o download de um arquivo de um bucket S3 usando o boto3 e exibe seu conteúdo linha por linha.

    Parâmetros:
    - uri: Caminho do arquivo no S3.
    - bucket_name: Nome do bucket S3.
    """

    s3_client = boto3.client('s3')

    # Faz o download do arquivo para um buffer de memória
    buffer = io.BytesIO()
    s3_client.download_fileobj(bucket_name, uri, buffer)

    # Lê o conteúdo do arquivo como texto e exibe linha por linha
    buffer.seek(0)
    with io.TextIOWrapper(buffer, encoding='utf-8') as f:
        for line in f:
            print(line.strip())

    

def main():

    create_dir('downloads')

    bucket_name = "commoncrawl"
    key = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"  # Caminho do arquivo

    # Download e leitura do arquivo .gz
    file_content = download_gz_file(bucket_name, key)
    
    # Verifica se o conteúdo foi baixado com sucesso
    if file_content:

        # Extrai o URI da primeira linha
        first_uri = extract_first_uri(file_content)
        if first_uri:
            print(f'Primeiro URI extraído: {first_uri}')

            # Baixa e imprime o conteúdo do arquivo da URI extraída
            download_file_from_s3(first_uri, bucket_name)
        else:
            print('Nenhuma URI extraida')
    else:
        print('Falha no download ou leitura do arquivo')


    # Pausa para visualização
    time.sleep(100)

if __name__ == "__main__":
    main()
