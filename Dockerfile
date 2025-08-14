# Use official Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your collector script
COPY bingx_async_collector.py .

# Set UTF-8 encoding (avoid Unicode errors)
ENV PYTHONIOENCODING=utf-8

# Run the script
CMD ["python", "bingx_async_collector.py"]
