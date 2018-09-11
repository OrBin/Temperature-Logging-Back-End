"""
This module serves the API for the temperature logging system's back-end.
"""

import json
import os
from datetime import datetime
from flask import Flask, jsonify, abort, request
from elasticsearch_dsl.connections import connections

from data_classes import Logger, Log
from logger_manager import LoggerManager


with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.json')) as config_file:
    config = json.loads(config_file.read())

app = Flask(__name__)
app.config['DEBUG'] = config['debug']

client = connections.create_connection(hosts=[config['db_host']])
LoggerManager.initialize()


@app.route('/latest', methods=['GET'], strict_slashes=False)
def get_latest_logs():
    """
    Returns the latest log data, as well as the logger's name, for each displayed logger
    """

    latest_logs = []
    for logger in LoggerManager.displayed_loggers:
        latest_log_search_results = logger.search_latest_log().execute()
        hits = latest_log_search_results.aggregations.latest_log.hits

        if len(hits) > 0:
            latest_log_dict = log_hit_to_dict(hits[0])
            del latest_log_dict['logger_id']
            latest_log_dict['logger_display_name'] = logger.display_name
            latest_logs.append(latest_log_dict)

    return jsonify(latest_logs)


DEFAULT_RETURNED_LOG_COUNT = 10
MAX_RETURNED_LOG_COUNT = 100
@app.route('/log', methods=['GET'], strict_slashes=False)
def get_logs():
    """
    Returns the latest DEFAULT_RETURNED_LOG_COUNT logs, or the requested count (if not above MAX_RETURNED_LOG_COUNT).
    If an argument 'count' is given and is above MAX_RETURNED_LOG_COUNT,
    this function will return MAX_RETURNED_LOG_COUNT logs.
    """
    try:
        log_count = int(request.args.get('count'))
        if log_count > MAX_RETURNED_LOG_COUNT:
            log_count = MAX_RETURNED_LOG_COUNT
    except:
        log_count = DEFAULT_RETURNED_LOG_COUNT

    search = Log.search() \
                    .sort('-timestamp') \
                    .params(size=log_count)
    results = search.execute()

    logs = [log_hit_to_dict(hit) for hit in results]
    return jsonify(logs)


@app.route('/log', methods=['POST'], strict_slashes=False)
def add_log():
    req_body = request.get_json()
    new_log = LoggerManager.all_loggers[req_body['logger']].add_log(
        timestamp=datetime.now(),
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
        'updatedAt': hit.timestamp,
        'humidity': hit.humidity,
        'heat_index_celsius': hit.heat_index_celsius,
        'temperature_celsius': hit.temperature_celsius,
    }


if __name__ == '__main__':
    app.run()
