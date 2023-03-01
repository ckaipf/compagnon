from typing import List

import yaml

import compagnon.domain.model as model


def record_representer(
    dumper: yaml.SafeDumper, record: model.Record
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!Record",
        {
            "foreign_id": record.foreign_id,
            "creation_time": record.creation_time,
            "data": record.data,
            "executions": record.executions,
        },
    )


def execution_representer(
    dumper: yaml.SafeDumper, execution: model.ExecutionFactory
) -> yaml.nodes.MappingNode:
    return dumper.represent_mapping(
        "!Execution",
        {
            "execution_id": execution.execution_id,
            "record": execution.record,
            "creation_time": execution.creation_time,
            "execution_name": execution.execution_name,
            "result": execution.result,
        },
    )


def get_dumper():
    safe_dumper = yaml.SafeDumper
    safe_dumper.add_representer(model.Record, record_representer)
    safe_dumper.add_representer(model.ExecutionFactory, execution_representer)
    return safe_dumper


def record_constructor(
    loader: yaml.SafeLoader, node: yaml.nodes.MappingNode
) -> model.Record:
    return model.Record(**loader.construct_mapping(node))  # type: ignore


def execution_constructor(
    loader: yaml.SafeLoader, node: yaml.nodes.MappingNode
) -> model.ExecutionFactory:
    return model.ExecutionFactory(**loader.construct_mapping(node))  # type: ignore


def get_loader():
    loader = yaml.SafeLoader
    loader.add_constructor("!Record", record_constructor)
    loader.add_constructor("!Execution", execution_constructor)
    return loader


class YamlDataBase:
    def __init__(self, file_path):
        self.file_path = file_path

    def load(self) -> List[model.Record]:
        try:
            with open(self.file_path, encoding="utf-8") as file_path:
                records: List[model.Record]
                records = yaml.load(file_path, Loader=get_loader())
                for record in records:
                    if not isinstance(record, model.Record):
                        raise TypeError(f"Imported record has type {type(record)}")
                    for execution in record.executions:
                        if not isinstance(execution, model.ExecutionFactory):
                            raise TypeError(
                                f"Imported execution has type {type(execution)}"
                            )
                return records
        except IOError:
            return []

    def dump(self, records: List[model.Record]):
        with open(self.file_path, "w+", encoding="utf-8") as file_path:
            dump = yaml.dump(records, Dumper=get_dumper())
            file_path.write(dump)
