FROM python:3.7-alpine

COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt && pip install gunicorn

CMD gunicorn -b 0.0.0.0:80 api_server:app
