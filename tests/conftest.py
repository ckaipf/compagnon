# pylint: disable=redefined-outer-name

import pytest

# from requests.exceptions import ConnectionError
from sqlalchemy import create_engine
from sqlalchemy.orm import clear_mappers, sessionmaker

# import requests
from compagnon import config
from compagnon.adapters.orm import metadata, start_mappers
from compagnon.domain.model import ExecutionFactory

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


#
#
#


@pytest.fixture()
def add_one_execution():
    addition = ExecutionFactory
    addition.add_data_parser(lambda x: x["x"])
    addition.add_command(lambda x: x + 1)
    addition.add_result_parser(lambda x: {"y": x})
    addition.add_execution_name("addition")
    return addition
