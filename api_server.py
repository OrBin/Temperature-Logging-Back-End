"""
This module serves the API for the temperature logging system's back-end.
"""

import json
import os
from flask import Flask, jsonify
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import Q

from data_classes import Logger


with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'config.json')) as config_file:
    config = json.loads(config_file.read())

app = Flask(__name__)
app.config['DEBUG'] = config['debug']
client = connections.create_connection(hosts=[config['db_host']])


@app.route('/latest', methods=['GET'])
def get_latest():
    """
    Returns the latest log data, as well as the logger's name, for each displayed logger
    """
    search = Logger.search()\
                    .query('bool', must=[], filter=[Q('term', is_displayed=True)])
    print(str(search.to_dict()).replace('\'', '"'))

    loggers_results = search.execute()

    loggers = []
    for logger in loggers_results:
        logger_object = Logger(meta={'id': logger.meta.id})
        latest_log_search_results = logger_object.search_latest_log().execute()
        latest_log = latest_log_search_results.aggregations.latest_log.hits[0]

        loggers.append({
            'logger_display_name': logger.display_name,
            'updatedAt': latest_log.timestamp,
            'humidity': latest_log.humidity,
            'heat_index_celsius': latest_log.heat_index_celsius,
            'temperature_celsius': latest_log.temperature_celsius,
        })

    return jsonify(loggers)


if __name__ == '__main__':
    app.run()
