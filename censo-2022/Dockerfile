FROM python:3.9

RUN python -m pip install --upgrade pip

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .
ENV OPENSSL_CONF=/resources/openssl.cnf

CMD ["python3", "/scripts/etl_censo2022.py"]