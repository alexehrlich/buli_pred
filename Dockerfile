
FROM selenium/base

USER root

RUN apt-get update && apt-get install -y python3 python3-pip
COPY . /app

RUN pip install -r /app/requirements.txt

WORKDIR /app/src/app


CMD ["fastapi", "run", "main.py", "--port", "80"]