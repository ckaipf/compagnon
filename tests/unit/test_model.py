import datetime
import random
import string
import unittest

from compagnon.domain.model import ExecutionFactory, Record


class ModelTestCase(unittest.TestCase):
    def setUp(self):
        class Addition(ExecutionFactory):
            execution_name = "addition"

            def data_parser(cls, x):
                return x["x"]

            def command(cls, x):
                return x + 1

            def result_parser(cls, x):
                return {"y": x}

        self.Addition = Addition

        class Subtraction(ExecutionFactory):
            execution_name = "subtraction"

            def data_parser(cls, x):
                return x["x"]

            def command(cls, x):
                return x - 1

            def result_parser(cls, x):
                return {"y": x}

        self.Subtraction = Subtraction

    def test_execution(self):
        record = Record(
            foreign_id="abc", data={"x": 1}, creation_time=datetime.datetime.now()
        )
        test_addition = self.Addition(record, creation_time=datetime.datetime.now())
        test_addition.execute()
        assert test_addition.result == {"y": 2}

    def test_result_is_immutable(self):
        record = Record(
            foreign_id="abc", data={"x": 1}, creation_time=datetime.datetime.now()
        )
        test_addition = self.Addition(record, creation_time=datetime.datetime.now())
        test_addition.execute()
        self.assertRaises(AttributeError, test_addition.execute)
        assert test_addition.result == {"y": 2}

    def test_add_execution_to_record(self):
        record = Record(
            foreign_id="abc", data={"x": 1}, creation_time=datetime.datetime.now()
        )
        e = self.Addition(record, creation_time=datetime.datetime.now())
        print("XX\n", type(e.execution_name), e.execution_name)
        record.add_execution(e)
        e.execute()
        assert record.executions[-1].result == {"y": 2}

        e = self.Subtraction(record, creation_time=datetime.datetime.now())
        record.add_execution(e)
        e.execute()
        assert record.executions[-1].result == {"y": 0}
