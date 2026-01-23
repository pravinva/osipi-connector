FROM python:3.11-slim

WORKDIR /app
# Force rebuild for pagination testing changes - Jan 9, 2026

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mock_piwebapi ./mock_piwebapi

ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "mock_piwebapi.main:app", "--host=0.0.0.0", "--port=8080"]
