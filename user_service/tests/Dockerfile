FROM python:3.9-slim

WORKDIR /app

# Копируем ключи
COPY private_key.pem .
COPY public_key.pem .

# Копируем основной код
COPY user_service.py .
COPY requirements.txt .
COPY tests/mock_db.py DB/db.py 

# Копируем тесты
COPY tests/ tests/

# Установка зависимостей
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "-m", "pytest", "tests/", "-v"]