FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Создаем директорию для proto-файлов, если она еще не существует
RUN mkdir -p proto

# Копируем proto-файл
COPY ./proto/post_service.proto ./proto/

# Генерируем Python-код из proto-файла
RUN python -m grpc_tools.protoc -I./proto --python_out=./ --grpc_python_out=./ ./proto/post_service.proto

# Устанавливаем переменные окружения
ENV PYTHONUNBUFFERED=1 \
    USER_SERVICE_URL="http://user-service:5001" \
    POST_SERVICE_HOST="post-service" \
    POST_SERVICE_PORT="50051"

# Создаем директорию для ключей JWT
RUN mkdir -p /app/keys

# Запускаем сервис
CMD ["python", "api_gateway.py"]

# Указываем порт, который будет слушать контейнер
EXPOSE 5000