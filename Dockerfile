FROM python:3.12-slim

# Evita que o Python gere arquivos .pyc no docker e garante que o log saia em tempo real
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV TZ=America/Sao_Paulo

WORKDIR /app

# Primeiro copiamos apenas o requirements para aproveitar o cache de camadas do Docker
COPY requirements.txt .

# Instala dependências do sistema e o driver ODBC para SQL Server
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg2 \
    unixodbc \
    unixodbc-dev \
    g++ \
    tzdata \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-archive-keyring.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-archive-keyring.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala as dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Agora copiamos o restante do código (o .dockerignore filtrará a .venv)
COPY . .

# Garante a existência das pastas de trabalho
RUN mkdir -p imports output logs

# Define o comando de inicialização
CMD ["python3", "main.py"]