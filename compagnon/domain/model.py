from __future__ import (
    annotations,
)  # type annotate attribute with the class that does not yet exist (Record)

from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Dict, List, Optional

import compagnon.domain.events as events
from compagnon.domain.events import Event


@dataclass
class ExecutionFactory:
    record: Record
    creation_time: date
    execution_id: str = ""
    result: Dict[str, Any] = field(default_factory=dict)
    execution_name: str = ""
    command: Callable = lambda x: x
    result_parser: Callable = lambda x: x
    data_parser: Callable = lambda x: x

    def __post_init__(self):
        self.execution_id = "_".join(
            [
                self.__class__.execution_name,
                str(self.record.foreign_id),
                self.creation_time.strftime("%Y%m%d%H%M%S"),
            ]
        )

    @classmethod
    def add_execution_name(cls, execution_name: str):
        cls.execution_name = execution_name

    @classmethod
    def add_command(cls, command: Callable):
        cls.command = command

    @classmethod
    def add_result_parser(cls, result_parser: Callable):
        cls.result_parser = result_parser

    @classmethod
    def add_data_parser(cls, data_parser: Callable):
        cls.data_parser = data_parser

    # def add_execution_id(self, execution_id: str):
    #     self.execution_id = execution_id

    def execute(self):
        if self.result:
            raise AttributeError("Execution already executed.")

        self.result = self.__class__.result_parser(
            self.__class__.command(self.__class__.data_parser(self.record.data))
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
        execution.execute()
        self.executions.append(execution)
        self.events.append(events.AddExecution())
        return

    def get_execution_ids(self) -> List[str]:
        return [e.execution_id for e in self.executions]
