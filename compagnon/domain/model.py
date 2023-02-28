from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List


@dataclass
class ExecutionFactory:
    execution_id: str
    result: Dict[str, Any] = field(default_factory=dict)
    execution_name: str = ""
    command: Callable = lambda x: x
    result_parser: Callable = lambda x: x
    data_parser: Callable = lambda x: x

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

    def add_execution_id(self, execution_id: str):
        self.execution_id = execution_id

    def execute(self, data):
        if self.result:
            raise AttributeError("Execution already executed.")

        self.result = self.__class__.result_parser(
            self.__class__.command(self.__class__.data_parser(data))
        )


@dataclass
class Record:
    foreign_id: str
    data: Dict[str, Any]
    executions: List[ExecutionFactory] = field(default_factory=list)

    def add_execution(self, execution: ExecutionFactory):
        execution.execute(self.data)
        self.executions.append(execution)
        return

    def get_execution_ids(self) -> List[str]:
        return [e.execution_id for e in self.executions]
