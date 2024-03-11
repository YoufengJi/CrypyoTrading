#   单例模式config_manager类
#   singleton class config_manager

import os
import sys

import yaml


class ConfigManager:
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(ConfigManager, cls).__new__(cls)
            try:
                cls.__instance.config = {}
                cls.__instance.load_config("config.yaml")
            except FileNotFoundError:
                sys.exit("Config load error: config.yaml not found")
            except yaml.YAMLError as e:
                sys.exit(f"Config load error: {e}")
            if not cls.__instance.config:
                sys.exit("Config load error: config.yaml is empty")

        return cls.__instance

    def load_config(self, path, ):
        with open(path, "r", encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

    def get_config(self, key):
        return self.config[key]
