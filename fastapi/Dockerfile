FROM node:20-bookworm

# Playwright installieren (bleibt gecached, solange base-image gleich)
RUN npx -y playwright@1.55.0 install --with-deps

USER root

RUN apt-get update && apt-get install -y python3-pip make python3.11-venv

WORKDIR /app

# 1️⃣ requirements.txt zuerst kopieren – wichtig für Cache
COPY requirements.txt .

# 2️⃣ Virtuelle Umgebung anlegen und requirements installieren
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# 3️⃣ Jetzt erst den restlichen Code kopieren
COPY . .

# 4️⃣ Port freigeben
EXPOSE 8000

# 5️⃣ Arbeitsverzeichnis setzen
WORKDIR /app/src/app

# 6️⃣ Startbefehl
CMD ["python", "-u", "-m", "fastapi", "run", "main.py", "--port", "8000"]