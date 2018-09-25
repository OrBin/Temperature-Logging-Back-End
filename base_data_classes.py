"""
This module contains base classes for the index data classes.
"""

from elasticsearch_dsl import Document, Join


class LoggerLogBase(Document):
    """
    A base class for :class:`~data_classes.Log` and :class:`~data_classes.Logger` data classes.
    """

    logger_log = Join(relations={'logger': 'log'})

    @classmethod
    def _matches(cls, hit):
        """
        Returns whether a hit matches this class or not.
        """
        return False

    class Index:
        """
        Meta-class for defining the index name.
        """
        name = 'logger-log'
