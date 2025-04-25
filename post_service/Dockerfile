FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p proto

COPY ./proto/post_service.proto ./proto/

RUN python -m grpc_tools.protoc -I./proto --python_out=./ --grpc_python_out=./ ./proto/post_service.proto

ENV PYTHONUNBUFFERED=1 \
    DATABASE_URL="postgresql://post_service:post_password@post-db:5432/posts_db" \
    GRPC_PORT=50051

RUN mkdir -p /var/log/post_service

CMD ["python", "post_service/post_service.py"]

EXPOSE 50051