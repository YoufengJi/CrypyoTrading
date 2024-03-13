import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime


class SingletonType(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonType, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Logger(metaclass=SingletonType):
    def __init__(self, log_dir='log'):
        self.logger = logging.getLogger('CryptoTradingLogger')
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            os.makedirs(log_dir, exist_ok=True)
            handler = TimedRotatingFileHandler(
                os.path.join(log_dir, datetime.now().strftime('%Y-%m-%d_%H-%M-%S.log')),
                when='midnight',
                interval=1,
                backupCount=30
            )
            handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(handler)

    def get_logger(self):
        return self.logger
