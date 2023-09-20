FROM python:3.11-slim

WORKDIR /src

COPY requirements.txt /src/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /src/requirements.txt

#COPY handle_data /src/handle_data
COPY show_data.py /src/show_data.py
COPY datasave.py /src/datasave.py
COPY insertdb.py /src/insertdb.py
COPY option_functions.py /src/option_functions.py
COPY paradigm.py /src/paradigm.py
COPY dflog.py /src/dflog.py

CMD ["python3", "show_data.py"]
EXPOSE 8050