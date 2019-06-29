"""
This module contains the index data classes.
"""

from elasticsearch_dsl import Date, Keyword, Boolean, Float

from base_data_classes import LoggerLogBase


class Logger(LoggerLogBase):
    """
    A class to represent a temperature logger.
    """
    name = Keyword()
    display_name = Keyword()
    is_displayed = Boolean()

    @classmethod
    def _matches(cls, hit):
        """
        Returns whether a hit matches this class or not.
        """
        return hit['_source']['logger_log'] == 'logger'

    @classmethod
    def search(cls, **kwargs):
        """
        Creates an :class:`~elasticsearch_dsl.Search` instance that will search
        over this index.
        """
        return cls._index.search(**kwargs).filter('term', logger_log='logger')

    @staticmethod
    def get_all():
        loggers = {}
        loggers_search = Logger.search()

        for logger_result in loggers_search.execute():
            logger_object = Logger(
                name=logger_result.name,
                display_name=logger_result.display_name,
                is_displayed=logger_result.is_displayed,
                meta={'id': logger_result.meta.id}
            )
            loggers[logger_result.meta.id] = logger_object

        return loggers

    def add_log(self, timestamp, humidity, temperature_celsius):
        """
        Save a new log which was logged by this logger.
        """
        log = Log(
            _routing=self.meta.id,
            logger_log={'name': 'log', 'parent': self.meta.id},
            timestamp=timestamp,
            humidity=humidity,
            temperature_celsius=temperature_celsius
        )

        log.save()
        return log

    @staticmethod
    def get_displayed(all_loggers=None):
        if not all_loggers:
            all_loggers = Logger.get_all()

        displayed_loggers = [logger for logger in all_loggers.values() if logger.is_displayed]
        displayed_loggers = sorted(displayed_loggers, key=lambda logger: logger.display_name)

        return displayed_loggers

    def search_logs(self):
        """
        Returns the search for this logger's logs.
        """
        search = Log.search()
        search = search.filter('parent_id', type='log', id=self.meta.id)
        search = search.params(routing=self.meta.id)
        return search

    def search_latest_log(self):
        """
        Returns the search for this logger's latest log.
        """
        search = self.search_logs()\
                        .params(size=0)
        search.aggs.metric('latest_log',
                           'top_hits',
                           sort=[{'timestamp': {'order': 'desc'}}],
                           size=1)
        return search

    def save(self, using=None, index=None, validate=True, **kwargs):
        """
        Saves the document into elasticsearch.
        See documentation for elasticsearch_dsl.Document.save for more information.
        """
        self.logger_log = {'name': 'logger'}
        return super().save(using, index, validate, **kwargs)

    def serialize_to_dict(self):
        return {
            'id': self.meta.id,
            'name': self.name,
            'display_name': self.display_name,
            'is_displayed': self.is_displayed,
        }

class Log(LoggerLogBase):
    """
    A class to represent a single temperature measurement log.
    """
    timestamp = Date()
    humidity = Float()
    temperature_celsius = Float()

    @classmethod
    def _matches(cls, hit):
        """
        Returns whether a hit matches this class or not.
        """
        return isinstance(hit['_source']['logger_log'], dict) \
            and hit['_source']['logger_log'].get('name') == 'log'

    @classmethod
    def search(cls, using=None, **kwargs):
        """
        Creates an :class:`~elasticsearch_dsl.Search` instance that will search
        over this index.
        """
        return cls._index.search(using=using, **kwargs).exclude('term', logger_log='logger')

    @property
    def logger(self):
        """
        Returns the logger that logged this log.
        """
        if 'logger' not in self.meta:
            self.meta.logger = Logger.get(id=self.logger_log.parent, index=self.meta.index)
        return self.meta.logger

    def save(self, using=None, index=None, validate=True, **kwargs):
        """
        Saves the document into elasticsearch.
        See documentation for elasticsearch_dsl.Document.save for more information.
        """
        self.meta.routing = self.logger_log.parent
        return super().save(using, index, validate, **kwargs)
