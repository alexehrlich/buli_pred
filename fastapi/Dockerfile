FROM node:20-bookworm

RUN npx -y playwright@1.56.0 install --with-deps

USER root

RUN apt-get update && apt-get install -y python3-pip make python3.11-venv

WORKDIR /app

COPY requirements.txt .

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8000

WORKDIR /app/src/app

CMD ["python", "-u", "-m", "fastapi", "run", "main.py", "--port", "8000"]