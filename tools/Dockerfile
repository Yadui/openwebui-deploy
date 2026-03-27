FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .

# Install deps and clean cache
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["uvicorn", "charting:app", "--host", "0.0.0.0", "--port", "9000"]
