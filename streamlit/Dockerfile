FROM python:3.11-slim

WORKDIR /app

#two Copy stages for caching efficiency 
COPY requirements.txt . 

RUN pip install -r requirements.txt

#copy the rest --> doesnt install all the dependencies all the time after a changge of the source files
COPY . .

#tell Docker which port we use
EXPOSE 8501 

#--server.port=8501: actutally tell streamlit which port to bind
CMD ["streamlit", "run", "src/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
