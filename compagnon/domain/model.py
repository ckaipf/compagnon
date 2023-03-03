from __future__ import (
    annotations,
)  # type annotate attribute with the class that does not yet exist (Record)

import abc
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Dict, List, Optional

import compagnon.domain.events as events
from compagnon.domain.events import Event


@dataclass
class ExecutionFactory(abc.ABC):
    record: Record
    creation_time: date
    execution_id: str = ""
    result: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        self.execution_id = "_".join(
            [
                self.__class__.execution_name,
                str(self.record.foreign_id),
                self.creation_time.strftime("%Y%m%d%H%M%S"),
            ]
        )

    @property
    @abc.abstractmethod
    def execution_name(cls):
        raise NotImplementedError

    @abc.abstractclassmethod
    def command(cls, x):
        raise NotImplementedError

    @abc.abstractclassmethod
    def result_parser(cls, x):
        raise NotImplementedError

    @abc.abstractclassmethod
    def data_parser(cls, x):
        raise NotImplementedError

    def execute(self):
        if self.result:
            raise AttributeError("Execution already executed.")

        class_ = self.__class__

        self.result = class_.result_parser(
            self, class_.command(self, class_.data_parser(self, self.record.data))
        )


@dataclass
class Record:
    foreign_id: str
    data: Dict[str, Any]
    creation_time: date
    executions: List[ExecutionFactory] = field(default_factory=list)
    events: List[Event] = field(default_factory=list)

    def __post_init__(self):
        self.events.append(events.AddRecord())

    def add_execution(self, execution: ExecutionFactory):
        self.executions.append(execution)
        self.events.append(events.AddExecution())
        return

    def get_execution_ids(self) -> List[str]:
        return [e.execution_id for e in self.executions]
