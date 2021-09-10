FROM python:3.7.9-slim

WORKDIR /code

COPY requirements.txt requirements.txt

RUN apt-get update \
    && apt-get install gcc -y \
    && apt-get clean
RUN pip install -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]