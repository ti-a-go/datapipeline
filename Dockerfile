FROM spark:3.5.4-scala2.12-java17-python3-ubuntu

WORKDIR /usr/app

COPY requirements.txt .

USER root

RUN pip install --no-cache-dir -r requirements.txt && \
    wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

CMD [ "python3", "src/main.py" ]
