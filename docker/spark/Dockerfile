# Custom spark image with sklearn + pandas + joblib + numpy for ML training.
# Wheels pre-downloaded to ./wheels/ (offline install in build).
# Total wheels ~76 MB; installs in seconds (no apt / no PyPI during build).
#
# Built image: smart-scenic/spark:custom
# Used by docker-compose.yml spark-master + spark-worker-1 service.

FROM apache/spark:3.4.1

USER root

# Install Python ML deps from local wheels only (offline, fast, deterministic)
COPY wheels/ /tmp/wheels/
RUN pip install --no-cache-dir --no-index --find-links=/tmp/wheels/ \
        scikit-learn==1.3.2 pandas==2.0.3 joblib==1.3.2 numpy==1.24.4 && \
    rm -rf /tmp/wheels/

# Quick sanity check
RUN python3 -c "import sklearn, pandas, joblib, numpy; print('sklearn', sklearn.__version__)"

# Custom entrypoints (run Master/Worker as foreground PID 1)
COPY entrypoint.sh      /usr/local/bin/entrypoint.sh
COPY entrypoint.worker.sh /usr/local/bin/entrypoint.worker.sh
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/entrypoint.worker.sh

USER root
