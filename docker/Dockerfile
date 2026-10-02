FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd --system --uid 10001 app
USER app
EXPOSE 8000
ENTRYPOINT ["python", "-m", "messaging"]
CMD ["--port", "8000", "--gateway", "gw-1"]
