FROM python:3.12-slim

LABEL maintainer="apu-saha-990"
LABEL project="WalletDNA"
LABEL description="Behavioural wallet fingerprinting and cluster detection"

# System deps
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer cache)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY walletdna/ ./walletdna/
COPY scripts/ ./scripts/

# Create runtime directories
RUN mkdir -p /app/data /app/logs

# Non-root user
RUN useradd -m -u 1000 walletdna && chown -R walletdna:walletdna /app
USER walletdna

EXPOSE 8000

CMD ["python", "-m", "walletdna.main"]
