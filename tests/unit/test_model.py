import datetime
import random
import string
import unittest

from compagnon.domain.model import ExecutionFactory, Record


class AdditionExecutionFactory(ExecutionFactory):
    pass


class SubtractionExecutionFactory(ExecutionFactory):
    pass


class ModelTestCase(unittest.TestCase):
    def setUp(self):
        self.addition = AdditionExecutionFactory
        self.addition.add_data_parser(lambda x: x["x"])
        self.addition.add_command(lambda x: x + 1)
        self.addition.add_result_parser(lambda x: {"y": x})

        self.subtraction = SubtractionExecutionFactory
        self.subtraction.add_data_parser(lambda x: x["x"])
        self.subtraction.add_command(lambda x: x - 1)
        self.subtraction.add_result_parser(lambda x: {"y": x})

    def test_configuration_of_execution(self):
        assert self.addition.data_parser({"x": 1}) == 1
        assert self.addition.command(1) == 2
        assert self.addition.result_parser(2) == {"y": 2}

    def test_execution(self):
        record = Record(
            foreign_id="abc", data={"x": 1}, creation_time=datetime.datetime.now()
        )
        test_addition = self.addition(record, creation_time=datetime.datetime.now())
        test_addition.execute()
        assert test_addition.result == {"y": 2}

    def test_result_is_immutable(self):
        record = Record(
            foreign_id="abc", data={"x": 1}, creation_time=datetime.datetime.now()
        )
        test_addition = self.addition(record, creation_time=datetime.datetime.now())
        test_addition.execute()
        self.assertRaises(AttributeError, test_addition.execute)
        assert test_addition.result == {"y": 2}

    def test_add_execution_to_record(self):
        record = Record(
            foreign_id="abc", data={"x": 1}, creation_time=datetime.datetime.now()
        )
        e = self.addition(record, creation_time=datetime.datetime.now())
        record.add_execution(e)
        e.execute()
        assert record.executions[-1].result == {"y": 2}

        e = self.subtraction(record, creation_time=datetime.datetime.now())
        record.add_execution(e)
        e.execute()
        assert record.executions[-1].result == {"y": 0}
