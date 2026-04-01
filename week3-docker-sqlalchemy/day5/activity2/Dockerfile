FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc libpq-dev  && apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/* .
COPY data data

ENV PYTHONUNBUFFERED=1

#CMD ["sh", "-c", "python app.py && pip freeze"]
CMD ["python", "app.py"]
