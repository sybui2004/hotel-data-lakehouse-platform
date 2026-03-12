FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements-docker.txt .

RUN pip install --no-cache-dir -r requirements-docker.txt

COPY main.py recommender.py ./

COPY models ./models
COPY static ./static
COPY templates ./templates

RUN mkdir -p data/train_test

COPY data/stopwords data/stopwords

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
