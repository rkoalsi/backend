# Use a lightweight Python image
FROM python:3.10-slim

# Set the working directory inside the container
WORKDIR /app/backend

# Copy all project files into the container
COPY . /app/backend

# Set PYTHONPATH to the parent of the 'backend' directory
ENV PYTHONPATH=/app

# WeasyPrint renders the estimate and distributor order PDFs. It binds to
# cairo/pango at runtime, which pip cannot supply.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpango-1.0-0 libpangoft2-1.0-0 libcairo2 libgdk-pixbuf-2.0-0 libffi-dev shared-mime-info \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# Expose the application port
EXPOSE 8000

# Command to run the FastAPI application
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
