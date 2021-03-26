import logging

class Logger(object, spark):
    def __init__(self, spark):
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')
        message_prefix = '<place for app and app_id >'
        self.logger = logging.getLogger(message_prefix)


    def error(self, message):
        self.logger.error(message)
        return None

    def warn(self, message):
        self.logger.warn(message)
        return None


    def info(self, message):
        self.logger.info(message)
        return None