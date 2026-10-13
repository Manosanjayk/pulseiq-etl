FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY metric_dictionary.csv ./metric_dictionary.csv
COPY src/ ./src/
COPY sql/ ./sql/
COPY tests/ ./tests/
CMD ["python", "src/etl.py"]
