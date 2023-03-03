import os
import unittest
from datetime import datetime

import compagnon.domain.model as model
import compagnon.service_layer.services as services
import compagnon.service_layer.unit_of_work as unit_of_work


class YamlUnitOfWorkTestCase(unittest.TestCase):
    def setUp(self):
        self.records = [
            model.Record(str(x), {"x": x}, creation_time=datetime.now())
            for x in range(5)
        ]
        self.yaml_file = "test.yaml"

        class Addition(model.ExecutionFactory):
            execution_name = "addition"

            def data_parser(self, x):
                return x["x"]

            def command(self, x):
                return x + 1

            def result_parser(self, x):
                return {"y": x}

        self.Addition = Addition

    def tearDown(self):
        os.remove(self.yaml_file)

    def test_add_records_to_uow(self):
        uow = unit_of_work.YamlUnitOfWork(self.yaml_file)
        services.add_records(self.records, uow)
        retrived = uow.records
        for record in self.records:
            assert retrived.get(record.foreign_id) is not None

    def test_add_execution_and_return_result(self):
        uow = unit_of_work.YamlUnitOfWork(self.yaml_file)
        services.add_records(self.records, uow)
        services.add_execution_to_records(self.Addition, uow)
        retrived = uow.records
        for record in self.records:
            first_execution = retrived.get(record.foreign_id).executions[0]
            first_execution.execute()
            assert first_execution.result["y"] == record.data["x"] + 1

    def test_save_and_load_records_with_executions_to_yaml(self):
        uow = unit_of_work.YamlUnitOfWork(self.yaml_file)
        services.add_records(self.records, uow)
        services.add_execution_to_records(self.Addition, uow)
        uow.commit()

        uow_ = unit_of_work.YamlUnitOfWork(self.yaml_file)
        assert id(uow_) != id(uow)

        with self.assertRaises(AttributeError):
            uow_.records

        with uow_:
            with uow:
                assert set(services.get_foreign_ids(uow_.records.list())) | set(
                    services.get_foreign_ids(uow.records.list())
                )

        for record in uow_.records.list():
            record_ = uow.records.get(record.foreign_id)
            assert set(record.get_execution_ids()) | set(record_.get_execution_ids())
