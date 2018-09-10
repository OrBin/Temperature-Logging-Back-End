"""
This module serves the API for the temperature logging system's back-end.
"""

import json
import os
from datetime import datetime
from flask import Flask, jsonify, abort, request
from elasticsearch_dsl.connections import connections

from data_classes import Logger


with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.json')) as config_file:
    config = json.loads(config_file.read())

app = Flask(__name__)
app.config['DEBUG'] = config['debug']
client = connections.create_connection(hosts=[config['db_host']])

loggers = Logger.get_all()
displayed_loggers = [logger for logger in loggers.values() if logger.is_displayed]
displayed_loggers = sorted(displayed_loggers, key=lambda logger: logger.display_name)


@app.route('/latest', methods=['GET'])
def get_latest_logs():
    """
    Returns the latest log data, as well as the logger's name, for each displayed logger
    """

    latest_logs = []
    for logger in displayed_loggers:
        latest_log_search_results = logger.search_latest_log().execute()
        latest_log = latest_log_search_results.aggregations.latest_log.hits[0]

        latest_logs.append({
            'logger_display_name': logger.display_name,
            'updatedAt': latest_log.timestamp,
            'humidity': latest_log.humidity,
            'heat_index_celsius': latest_log.heat_index_celsius,
            'temperature_celsius': latest_log.temperature_celsius,
        })

    return jsonify(latest_logs)


@app.route('/log', methods=['GET'])
def get_logs():
    abort(501) # Not implemented


@app.route('/log', methods=['POST'])
def add_log():
    req_body = request.get_json()
    new_log = loggers[req_body['logger']].add_log(
        timestamp=datetime.now(),
        heat_index_celsius=req_body['heat_index_celsius'],
        humidity=req_body['humidity'],
        temperature_celsius=req_body['temperature_celsius']
    )
    return f'Added the logger with id: {new_log.meta.id}'


@app.route('/logger', methods=['GET'])
def get_loggers():
    abort(501) # Not implemented


@app.route('/logger', methods=['POST'])
def add_logger():
    abort(501) # Not implemented


@app.route('/logger/:loggerId', methods=['GET'])
def get_logger():
    abort(501) # Not implemented


@app.route('/logger/:loggerId', methods=['PUT'])
def update_logger():
    abort(501) # Not implemented


if __name__ == '__main__':
    app.run()
