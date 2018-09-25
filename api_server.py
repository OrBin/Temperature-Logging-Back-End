"""
This module serves the API for the temperature logging system's back-end.
"""

import json
import os
import re
import elasticsearch.exceptions
from time import sleep
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, abort, request
from flask_cors import cross_origin
from elasticsearch_dsl.connections import connections

from data_classes import Logger, Log
from logger_manager import LoggerManager


DEFAULT_RETURNED_LOG_PERIOD_MINUTES = 60
MAX_RETURNED_LOG_PERIOD_MINUTES = 60*12
MAX_RETURNED_LOG_COUNT = 100
CONNECTION_RETRY_TIMEOUT_MINUTES = 5
CONNECTION_RETRY_INTERVAL_SECONDS = 10

with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.json')) as config_file:
    config = json.loads(config_file.read())

app = Flask(__name__)
app.config['DEBUG'] = config['debug']

started_connecting_time = datetime.now()

while (datetime.now() - started_connecting_time) < timedelta(minutes=CONNECTION_RETRY_TIMEOUT_MINUTES):
    try:
        print('Connecting to database...')
        client = connections.create_connection(hosts=[config['db_host']])
        LoggerManager.initialize()
        print('Connected successfully')
        break
    except (ConnectionRefusedError, elasticsearch.exceptions.ConnectionError):
        print(f'Failed to connect to database, retrying in {CONNECTION_RETRY_INTERVAL_SECONDS} seconds')
        sleep(CONNECTION_RETRY_INTERVAL_SECONDS)


@app.route('/latest', methods=['GET'], strict_slashes=False)
@cross_origin()
def get_latest_logs():
    """
    Returns the latest log data, as well as the logger's name, for each displayed logger
    """

    search = Logger.search() \
                    .query('match', is_displayed=True) \
                    .params(size=0)

    search.aggs.bucket('top-loggers', 'terms', field='_id') \
                .bucket('to-logs', 'children', type='log') \
                .metric('top-logs', 'top_hits', size=1, sort=[{ 'timestamp': 'desc' }])

    results = search.execute()
    logger_buckets = results.aggregations['top-loggers'].buckets
    latest_logs = []

    for bucket in logger_buckets:
        hits = bucket['to-logs']['top-logs'].hits

        if len(hits) > 0:
            latest_log_dict = log_hit_to_dict(hits[0])
            logger = LoggerManager.all_loggers[latest_log_dict['logger_id']]
            latest_log_dict['logger_display_name'] = logger.display_name
            del latest_log_dict['logger_id']
            latest_logs.append(latest_log_dict)

    latest_logs = sorted(latest_logs, key=lambda log: log['logger_display_name'])
    return jsonify(latest_logs)



@app.route('/log', methods=['GET'], strict_slashes=False)
@cross_origin()
def get_logs():
    """
    If no arguments 'period' or 'count' are given, returns the logs from the latest DEFAULT_RETURNED_LOG_PERIOD_MINUTES.

    If an argument 'period' is given, returns the logs from the latest period as required.
    The acceptable format for argument 'period' is an integer with one of the letters 's', 'm', 'h' or 'd',
    for example: '90s', '15m', '24h', '7d'.
    The maximum period is defined by MAX_RETURNED_LOG_PERIOD_MINUTES.

    If no argument 'period' is given, but an argument 'count' is given, returns the latest logs by the requested count.
    The acceptable format for argument 'count' is an integer.
    The maximum count is defined by MAX_RETURNED_LOG_COUNT.
    """

    get_logs_by_count = False

    if 'period' in request.args:
        match = re.match('(\d+[smhd])', request.args.get('period'))
        if match is None:
            abort(400) # Bad request

        logs_period = match.group(1)

        # Checking that logs_period is not above MAX_RETURNED_LOG_PERIOD_MINUTES
        TO_MINUTES = {
            's': 1/60,
            'm': 1,
            'h': 60,
            'd': 60*24
        }
        logs_period_minutes = int(logs_period[:-1]) * TO_MINUTES[logs_period[-1]]
        if logs_period_minutes > MAX_RETURNED_LOG_PERIOD_MINUTES:
            logs_period = f'{MAX_RETURNED_LOG_PERIOD_MINUTES}m'

    elif 'count' in request.args:
        get_logs_by_count = True
        try:
            logs_count = int(request.args.get('count'))
            if logs_count > MAX_RETURNED_LOG_COUNT:
                logs_count = MAX_RETURNED_LOG_COUNT
        except ValueError:
            # Raised when value for argument 'count' is not a number
            abort(400) # Bad request
    else:
        logs_period = f'{DEFAULT_RETURNED_LOG_PERIOD_MINUTES}m'

    search = Log.search().sort('-timestamp')

    if get_logs_by_count:
        search =  search.params(size=logs_count)
    else:
        search = search.filter('range', timestamp={'gte': f'now-{logs_period}', 'lte': 'now'})

    results = search.execute()
    logs = [log_hit_to_dict(hit) for hit in results]
    return jsonify(logs)


@app.route('/log', methods=['POST'], strict_slashes=False)
def add_log():
    req_body = request.get_json()
    new_log = LoggerManager.all_loggers[req_body['logger']].add_log(
        timestamp=datetime.now(tz=timezone.utc),
        heat_index_celsius=req_body['heat_index_celsius'],
        humidity=req_body['humidity'],
        temperature_celsius=req_body['temperature_celsius']
    )
    return f'Added a log with id: {new_log.meta.id}'


@app.route('/logger', methods=['GET'], strict_slashes=False)
def get_loggers():
    displayed_only = request.args.get('displayed_only') == 'true'

    if displayed_only:
        loggers_list_to_iterate = LoggerManager.displayed_loggers
    else:
        loggers_list_to_iterate = LoggerManager.all_loggers.values()

    return jsonify([logger.serialize_to_dict() for logger in loggers_list_to_iterate])


@app.route('/logger', methods=['POST'], strict_slashes=False)
def add_logger():
    req_body = request.get_json()

    new_logger = Logger(
        name=req_body['name'],
        display_name=req_body['display_name'],
        is_displayed=req_body['is_displayed']
    )
    new_logger.save()

    LoggerManager.all_loggers[new_logger.meta.id] = new_logger
    LoggerManager.displayed_loggers = Logger.get_displayed(LoggerManager.all_loggers)

@app.route('/logger/<string:logger_id>', methods=['GET'], strict_slashes=False)
def get_logger(logger_id):
    if logger_id in LoggerManager.all_loggers:
        return jsonify(LoggerManager.all_loggers[logger_id].serialize_to_dict())
    else:
        abort(404) # Not found


@app.route('/logger/<string:logger_id>', methods=['PUT'], strict_slashes=False)
def update_logger(logger_id):
    req_body = request.get_json()

    if logger_id in LoggerManager.all_loggers:
        logger_to_update = LoggerManager.all_loggers[logger_id]
    else:
        abort(404) # Not found

    if 'name' in req_body:
        logger_to_update.name = req_body['name']

    if 'display_name' in req_body:
        logger_to_update.display_name = req_body['display_name']

    if 'is_displayed' in req_body:
        logger_to_update.is_displayed = req_body['is_displayed']
        LoggerManager.refresh_displayed_loggers()

    logger_to_update.save()

    return jsonify(logger_to_update.serialize_to_dict())


def log_hit_to_dict(hit):
    return {
        'logger_id': hit.meta.routing,
        'updatedAt': datetime.fromtimestamp(hit.timestamp.timestamp(), tz=timezone.utc),
        'humidity': hit.humidity,
        'heat_index_celsius': hit.heat_index_celsius,
        'temperature_celsius': hit.temperature_celsius,
    }


if __name__ == '__main__':
    app.run()
