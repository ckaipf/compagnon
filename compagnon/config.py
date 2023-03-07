import yaml
from typing import Dict, List
import functools
def get_postgres_uri():
    host = "localhost"
    port = 5432
    password = "postgres"
    user, db_name = "postgres", "loop"
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

def get_config(path='config.yml'):
    with open(path, "r") as stream:
        try:
            return(yaml.safe_load(stream))
        except yaml.YAMLError as exc:
            print(exc)


def get_nested(dictionary: Dict, path: List[str]):
    return functools.reduce(lambda dict_, key: dict_[key], path, dictionary)

def add_config(file_path="config.yml", dict_path=[]):
    config = get_config(file_path)
    
    def decorate(function):
        d = [function.__module__] + dict_path
        config_ = get_nested(config, d)
        
        def wrap_function(*args, **kwargs):
            kwargs = config_ | kwargs
            return function(*args, **kwargs)
        return wrap_function

    return decorate


config = get_config()