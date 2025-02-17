# # Utilise l'image Python 3.9 en version slim
# FROM python:3.9-slim

# # Définit le répertoire de travail dans le conteneur
# WORKDIR /app

# # Copie le fichier des dépendances et l'installe
# COPY requirements.txt requirements.txt
# RUN pip install --no-cache-dir -r requirements.txt

# # Copie l'ensemble de votre code dans le conteneur
# COPY . .

# # Expose le port (facultatif, utile si votre app écoute sur un port)
# EXPOSE 5550

# # Définit la commande de lancement de votre application
# CMD ["python", "app.py"]






# Utilise l'image Python 3.9 en version slim
FROM python:3.9-slim

# Met à jour les dépôts, installe le JRE par défaut (OpenJDK 17 sur Bookworm) et procps pour la commande ps
RUN apt-get update && \
    apt-get install -y default-jre-headless procps && \
    apt-get clean

# Définir JAVA_HOME et mettre à jour le PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier le fichier des dépendances et l'installer
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copier l'ensemble de votre code dans le conteneur
COPY . .

# Exposer le port (facultatif, utile si votre application écoute sur un port)
EXPOSE 5550

# Définit la commande de lancement de votre application
CMD ["python", "consumer.py"]
