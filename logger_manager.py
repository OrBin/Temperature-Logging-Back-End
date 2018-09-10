from data_classes import Logger


class LoggerManager:
    all_loggers = None
    displayed_loggers = None

    @staticmethod
    def initialize():
        LoggerManager.all_loggers = Logger.get_all()
        LoggerManager.displayed_loggers = Logger.get_displayed(LoggerManager.all_loggers)

    @staticmethod
    def refresh_displayed_loggers():
        LoggerManager.displayed_loggers = Logger.get_displayed(LoggerManager.all_loggers)
