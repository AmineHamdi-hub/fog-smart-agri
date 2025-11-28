# Utilise une image Python légère comme base
FROM python:3.10-slim

# Définit le répertoire de travail, conventionnellement /app
WORKDIR /app

# Copie le fichier de dépendances spécifique et les installe
# Assurez-vous que requirements.txt est présent dans le même dossier que le Dockerfile
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie le fichier d'entrée du microservice
# (Ex: ingestor_main.py, predictor_main.py, ou decision_main.py)
COPY *.py /app/

# Le CMD final est surchargé par le 'command' du docker-compose.yaml