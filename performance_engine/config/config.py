import json
from pathlib import Path

class PerformanceConfiguration:
    """
    The responsibility of this class is to pull in configuration from a file and make it available across the
    application
    """
    global_config = {}

    @classmethod
    def set_global_config(cls, path=None, **kwargs):
        if path:
            with open(path, 'r') as fp:
                cls.global_config = dict(json.load(fp))
        cls.global_config.update(kwargs)

    @classmethod
    def item(cls, key, default=None):
        return cls.global_config.get(key, default)

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def __str__(self):
        return str(self.__dict__)

    def __iter__(self):
        return self.__dict__.__iter__()

    def to_dict(self):
        return self.__dict__

    def get(self, key, default=None):
        return self.__dict__.get(key, self.global_config.get(key, default))

    def __getattr__(self, item):
        return self.get(item)


global_config = PerformanceConfiguration()
global_config.set_global_config(Path(__file__).parent.joinpath("config.json"))
