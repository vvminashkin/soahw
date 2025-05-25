FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p proto

COPY ./proto/statistics_service.proto ./proto/

RUN python -m grpc_tools.protoc -I./proto --python_out=./ --grpc_python_out=./ ./proto/statistics_service.proto

ENV PYTHONUNBUFFERED=1 \
    CLICKHOUSE_HOST="clickhouse" \
    CLICKHOUSE_PORT="9000" \
    CLICKHOUSE_USER="default" \
    CLICKHOUSE_PASSWORD="" \
    CLICKHOUSE_DB="stats_db" \
    KAFKA_BOOTSTRAP_SERVERS="kafka:9092" \
    GRPC_PORT="50052"

CMD ["python", "statistics_service.py"]

EXPOSE 50052
