# pylint: disable=redefined-outer-name

from datetime import datetime

import pytest
# from requests.exceptions import ConnectionError
from sqlalchemy import create_engine
from sqlalchemy.orm import clear_mappers, sessionmaker

# import requests
from compagnon import config
from compagnon.adapters.orm import metadata, start_mappers
from compagnon.domain.model import AbstractExecution, Record

pytest_plugins = ["conftest_cogdat_integration"]


@pytest.fixture
def in_memory_db():
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine


@pytest.fixture
def session_factory(in_memory_db):
    start_mappers()
    yield sessionmaker(bind=in_memory_db)
    clear_mappers()


# @pytest.fixture
# def session(session_factory):
#     return session_factory()


# def wait_for_postgres_to_come_up(engine):
#     deadline = time.time() + 10
#     while time.time() < deadline:
#         try:
#             return engine.connect()
#         except OperationalError:
#             time.sleep(0.5)
#     pytest.fail("Postgres never came up")


# @pytest.fixture(scope="session")
# def postgres_db():
#     engine = create_engine(config.get_postgres_uri())
#     wait_for_postgres_to_come_up(engine)
#     metadata.create_all(engine)
#     return engine


# @pytest.fixture
# def postgres_session(postgres_db):
#     start_mappers()
#     yield sessionmaker(bind=postgres_db)()
#     clear_mappers()


@pytest.fixture(scope="class")
def dummy_executions(request):
    class Addition(AbstractExecution):
        execution_name = "addition"


        def command(cls, x):
            return {"y": x["x"] + 1}


    class Subtraction(AbstractExecution):
        execution_name = "subtraction"


        def command(cls, x):
            return {"y": x["x"] - 1}


    class TimesTwo(AbstractExecution):
        execution_name = "times_two"

        def command(cls, x):
            return {"y": x["x"] * 2}

    request.cls.Addition = Addition
    request.cls.Subtraction = Subtraction
    request.cls.TimesTwo = TimesTwo


@pytest.fixture()
def one_record():
    return Record(str(1), data={"x": 1}, creation_time=datetime.datetime.now())
