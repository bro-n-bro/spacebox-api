FROM python:3.8

COPY requirements.txt .
RUN pip install -r ./requirements.txt

COPY db.py .
COPY main.py .
COPY config config/
COPY clients clients/
COPY common common/
COPY services services/


CMD python -u ./main.py --log=INFO
