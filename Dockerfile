FROM python:3.12-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir -r requirements.txt

# Expose environment variable secrets (ensure .env is included during runtime, not baked into the image)
RUN apt-get update && apt-get install -y --no-install-recommends gcc && apt-get clean

CMD ["python", "./producer.py"]
