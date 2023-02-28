import abc
import datetime
from typing import Any, Dict, List

import datameta_client

import compagnon.domain.model as model


class AbstractFetcher(abc.ABC):
    @abc.abstractmethod
    def to_record(self, raw_record: Dict) -> model.Record:
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, foreign_id) -> model.Record:
        raise NotImplementedError

    @abc.abstractmethod
    def list(self) -> List[model.Record]:
        pass


class CogdatFetcher(AbstractFetcher):
    def __init__(self):
        self.dm_client = datameta_client

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
            self.to_record(raw_record)
            for raw_record in self.dm_client.metadatasets.search()
        ]
