# version python
FROM python:3.10-slim

WORKDIR /

RUN apt-get update

# work dircetory
WORKDIR /app

# requirements.txt location
COPY source/requirements.txt /app/requirements.txt

# Run install requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# COPY DIR
COPY source /app/source

WORKDIR /app/source

ENV PYTHONUNBUFFERED=1

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8732", "--reload"]
