FROM python:3.10-slim

WORKDIR /app

COPY kafka/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY kafka/ .

CMD ["python", "-u", "producer.py"]