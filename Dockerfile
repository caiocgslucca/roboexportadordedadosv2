FROM python:3.11-slim

WORKDIR /app

# 🔹 Chrome/Chromium e dependências para o Selenium
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["gunicorn", "web_robo_exportador:create_app()", "--bind", "0.0.0.0:5000"]
