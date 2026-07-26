FROM python:3.11-slim

WORKDIR /app

COPY dbt/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY dbt/ /app/dbt/

CMD ["dbt", "run", "--project-dir", "/app/dbt", "--profiles-dir", "/app/dbt"]