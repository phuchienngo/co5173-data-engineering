FROM bitnamilegacy/spark:4.0

USER 0

COPY requirements.txt /tmp/requirements.txt
RUN grep -v '^pyspark' /tmp/requirements.txt > /tmp/runtime-requirements.txt \
    && pip install --no-cache-dir --quiet -r /tmp/runtime-requirements.txt \
    && rm -f /tmp/requirements.txt /tmp/runtime-requirements.txt
