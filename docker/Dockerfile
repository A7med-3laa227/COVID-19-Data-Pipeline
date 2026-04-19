# ── Stage: COVID-19 ETL Pipeline ──────────────────────────────────────────────
FROM python:3.10-slim

# Set working directory inside the container
WORKDIR /app

# Install dependencies first (cached layer)
COPY pipeline/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the pipeline code and SQL schema
COPY pipeline/explore_data.py .
COPY sql/schema.sql .

# Run the ETL pipeline
CMD ["python", "explore_data.py"]
