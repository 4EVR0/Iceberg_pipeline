FROM python:3.12-slim

ENV AWS_DEFAULT_REGION=ap-northeast-2
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x scripts/entrypoint.sh

ENTRYPOINT ["scripts/entrypoint.sh"]
