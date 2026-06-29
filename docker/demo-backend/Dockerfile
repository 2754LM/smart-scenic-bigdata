# ============================================================
# Custom demo-backend image (FastAPI + uvicorn)
# ============================================================
# Minimal Dockerfile - NO apt-get (no Java, no curl).
# PySpark in backend uses sklearn fallback (sklearn models trained by spark-master).
# Java only needed inside spark-master, not here.

FROM python:3.10-slim

USER root

WORKDIR /app

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