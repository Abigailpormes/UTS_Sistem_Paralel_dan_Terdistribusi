FROM python:3.11-slim
WORKDIR /app

# install build deps if needed (none here)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# create non-root user after installing packages
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app

COPY src ./src
USER appuser

EXPOSE 8080
CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
