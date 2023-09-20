FROM python:3.8.10-slim

WORKDIR /src

COPY requirements.txt /src/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /src/requirements.txt

COPY bastionmdc.py /src/bastionmdc.py

CMD ["python3", "bastionmdc.py"]
EXPOSE 8051