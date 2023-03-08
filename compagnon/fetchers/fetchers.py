import abc
import datetime
from typing import Any, Dict, List, Callable

from datameta_client import metadatasets
import datameta_client as dm_client
import compagnon.domain.model as model
import pathlib
import requests
import re

class AbstractFetcher(abc.ABC):
    @abc.abstractmethod
    def to_record(self, raw_record: Dict) -> model.Record:
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, foreign_id) -> model.Record:
        raise NotImplementedError

    @abc.abstractmethod
    def list(self) -> List[model.Record]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_file(self, record: model.Record, file_extractor: Callable, path_prefix: pathlib.Path) -> pathlib.Path:
        raise NotImplementedError


class CogdatFetcher(AbstractFetcher):
    def to_record(self, raw_record: Any) -> model.Record:
        return model.Record(
            foreign_id=raw_record["id"]["site"],
            data=raw_record,
            creation_time=datetime.datetime.now(),
        )

    def get(self, foreign_id) -> model.Record:
        raise NotImplementedError

    def list(self) -> List[model.Record]:
        return [
            self.to_record(raw_record) for raw_record in metadatasets.search(quiet=True)
        ]

    def get_file(self, record: model.Record, file_extractor: Callable, path_prefix: pathlib.Path) -> pathlib.Path:
          
        file_id = file_extractor(record)
        response = dm_client.files.download_url(file_id)
        file_url = response["file_url"]
    
        response = requests.get(file_url, stream=True)
        response.raise_for_status()      
        
        filename = re.findall("filename=(.+)", response.headers["content-disposition"]).pop().strip(' "')

        local_filename = path_prefix / filename
        with open(local_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return local_filename



    