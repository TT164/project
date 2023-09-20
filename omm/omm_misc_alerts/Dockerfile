FROM python:3.8.10-slim

ENV host=""
ENV port=""
ENV database=""
ENV user=""
ENV password=""
ENV account_user=""
ENV account_psd=""

WORKDIR /src

COPY requirements.txt /src/requirements.txt

RUN apt update

RUN apt-get install -y libpq-dev gcc g++

RUN pip install --no-cache-dir --upgrade -r /src/requirements.txt

COPY main.py /src/main.py

CMD ["python3", "main.py"]