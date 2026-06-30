# ============================================================
# Custom demo-backend image (FastAPI + uvicorn + pyhive)
# ============================================================
# Minimal Dockerfile - NO Java (needed only inside spark-master).
# pyhive needs SASL system libs for HiveServer2 Thrift SASL transport.

FROM python:3.10-slim

USER root

WORKDIR /app

# Install system deps needed by pyhive/thrift-sasl (libsasl2 for SASL handshake)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libsasl2-2 libsasl2-dev libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt /app/requirements.txt

# Install Python deps (use Tsinghua mirror for speed in China)
RUN pip install --no-cache-dir --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple && \
    pip install --no-cache-dir -r /app/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# Copy backend code (also mounted as volume at runtime, this is fallback for image)
COPY . /app

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
