FROM node:20-bookworm
RUN npx -y playwright@1.55.0 install --with-deps

USER root

RUN apt-get update && apt-get install -y python3-pip make python3.11-venv
COPY . /app

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -vvv -r /app/requirements.txt

RUN playwright install


WORKDIR /app/src/app


CMD ["fastapi", "run", "main.py", "--port", "8000"]