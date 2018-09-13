FROM python:3.7-alpine

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt && pip install gunicorn

CMD gunicorn api_server:app