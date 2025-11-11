FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY etc/requirements.txt /app/etc/requirements.txt
RUN pip install --no-cache-dir -r etc/requirements.txt

# Copy application code
COPY elktail/ /app/elktail/

# Create config directory
RUN mkdir -p /root/.config/elktail

# Set the entrypoint
ENTRYPOINT ["python", "-m", "elktail.elktail"]
