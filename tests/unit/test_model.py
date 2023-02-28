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
        test_addition = self.addition(
            execution_id="".join(random.choices(string.ascii_lowercase, k=5))
        )
        test_addition.execute(data={"x": 1})
        assert test_addition.result == {"y": 2}

    def test_result_is_immutable(self):
        test_addition = self.addition(
            execution_id="".join(random.choices(string.ascii_lowercase, k=5))
        )
        test_addition.execute(data={"x": 1})
        self.assertRaises(AttributeError, test_addition.execute, data={"x": 2})
        assert test_addition.result == {"y": 2}

    def test_add_execution_to_record(self):
        record = Record(foreign_id="abc", data={"x": 1})
        record.add_execution(
            self.addition(
                execution_id="".join(random.choices(string.ascii_lowercase, k=5))
            )
        )
        assert record.executions[0].result == {"y": 2}
        record.add_execution(
            self.subtraction(
                execution_id="".join(random.choices(string.ascii_lowercase, k=5))
            )
        )
        assert record.executions[1].result == {"y": 0}
