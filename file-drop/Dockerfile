# HELIOS filedrop-tools image (ADR-004): nightly CSV feed generator with dirt modes.
# Stdlib only at runtime; pytest rides along for `make test-drop`.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=0

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY pytest.ini ./
COPY filedrop/ ./filedrop/
COPY tests/ ./tests/

# Default: emit today's feeds into the SFTP-style drop volume.
CMD ["python", "-m", "filedrop.generate", "--report"]
