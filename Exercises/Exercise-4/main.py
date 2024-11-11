import os 
import pandas as pd
import json

def create_dir(path: str) -> None:
    """
    Criação do diretório de destino caso ele não exista 

    Parâmetros:
    - path: Caminho do diretório a ser criado.
    """

    os.makedirs(path, exist_ok=True)

def flatten_json(json_obj):
    """
    Achata um dicionário JSON recursivamente, transformando uma estrutura aninhada
    em um dicionário plano, onde as chaves representam a hierarquia original.
    
    Parâmetros:
    - json_obj (dict): O objeto Json a ser achatado

    Retorno:
    - flat_data (dict): Um dicionário plano com todas as chaves e valores do Json original
    """

    flat_data = {}
    
    def _flatten(obj, prefix=''):
        if isinstance(obj, dict):
            for k, v in obj.items():
                _flatten(v, prefix + k + '_')
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                _flatten(item, prefix + str(i) + '_')
        else:
            flat_data[prefix[:-1]] = obj
    
    _flatten(json_obj)
    return flat_data

def find_and_convert_json_to_csv(source_dir, target_dir):
    """
    Percorre o diretório 'source_dir' para localizar todos os arquivos JSON, lê cada arquivo achata 
    o contéudo  e converte em um arquivo csv no 'target_dir'

    Paramêtros:
    - source_dir (str): Diretório onde os arquivos JSON estão localizados 
    - target_dir (str): Diretório onde os arquivos CSV estão salvos

    """
    
    for root, dirs, files in os.walk(source_dir):
        for file in files:
            print(file)
            if file.endswith('.json'):

                source_file = os.path.join(root, file)

                try:
                    # Lê o conteúdo JSON do arquivo
                    with open(source_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)

                    if isinstance(data, list):
                        flattened_data = [flatten_json(item) for item in data]
                    else:
                        flattened_data = [flatten_json(data)]
                    
                    df = pd.DataFrame(flattened_data)
                    csv_filename = os.path.splitext(file)[0] + '.csv'
                    csv_path = os.path.join(target_dir, csv_filename)

                    df.to_csv(csv_path, index=False)
                    print(f'Arquivo Json convertido para CSV: {source_dir} -> {csv_path}')

                except Exception as e:
                    print(f'Erro ao processar o arquivo {source_file}: {e}')

def main():
    create_dir('json_files')

    source_dir = 'data'
    target_dir = 'json_files'

    find_and_convert_json_to_csv(source_dir, target_dir)

if __name__ == "__main__":
    main()
