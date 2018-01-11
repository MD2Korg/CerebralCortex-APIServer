import logging
import os
from CommonConstants import LoggerConstants

class LoggerUtility:
    @staticmethod
    def setLevel():
        logFormat = '%(asctime)-15s %(levelname)s:%(message)s'
        logging.basicConfig(format=logFormat)
        logger = logging.getLogger(LoggerConstants.LOGGER_NAME)

        try:
            logLevel = os.environ(LoggerConstants.LOGGER_LOG_LEVEL_ENV_VAR)
        except Exception as e:
            logLevel = LoggerConstants.LOGGER_DEFAULT_LOG_LEVEL

        logger.setLevel(logging.getLevelName(logLevel))
        return True

    @staticmethod
    def logInfo(message):
        logger = logging.getLogger(LoggerConstants.LOGGER_NAME)
        logger.info('%s', message)
        return True

    @staticmethod
    def logError(message):
        logger = logging.getLogger(LoggerConstants.LOGGER_NAME)
        logger.error('%s', message)
        return True

    @staticmethod
    def logWarning(message):
        logger = logging.getLogger(LoggerConstants.LOGGER_NAME)
        logger.warning('%s', message)
        return True
