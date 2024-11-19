# Baseado na imagem oficial do Bitnami Spark
FROM bitnami/spark:3.5.1

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia o arquivo requirements.txt para o contêiner
COPY requirements.txt /app/requirements.txt

# Atualiza pacotes e instala dependências do Python
RUN pip install --no-cache-dir -r /app/requirements.txt
