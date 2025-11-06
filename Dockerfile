# Use official Python runtime
FROM python:3.11-slim

# system deps for building
RUN apt-get update && apt-get install -y build-essential git curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# copy python requirements
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# copy app
COPY . .

# expose port
ENV PORT 8000
EXPOSE 8000

# startup
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
