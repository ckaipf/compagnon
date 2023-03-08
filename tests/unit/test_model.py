import datetime
import unittest

import pytest

from compagnon.domain.model import AbstractExecution, Record


@pytest.mark.usefixtures("dummy_executions")
class ModelTestCase(unittest.TestCase):
    def setUp(self):
        pass

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
        record.add_execution(e)
        e.execute()
        assert record.executions[-1].result == {"y": 2}

        e = self.Subtraction(record, creation_time=datetime.datetime.now())
        record.add_execution(e)
        e.execute()
        assert record.executions[-1].result == {"y": 0}
