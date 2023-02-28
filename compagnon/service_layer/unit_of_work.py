# pylint: disable=useless-parent-delegation

import abc

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session

import compagnon.adapters.repository as repository
from compagnon import config
from compagnon.adapters.yaml_database import YamlDataBase


class AbstractUnitOfWork(abc.ABC):
    records: repository.AbstractRepository

    def __enter__(self):
        pass

    def __exit__(self, *args):
        self.rollback()

    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self):
        raise NotImplementedError


class SetUnitOfWork(AbstractUnitOfWork):
    def __init__(self):
        self.records = repository.DictRepository([])
        self.committed = False

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


DEFAULT_SESSION_FACTORY = sessionmaker(
    bind=create_engine(
        config.get_postgres_uri(),
    )
)


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):
    def __init__(self, session_factory=DEFAULT_SESSION_FACTORY):
        self.session_factory = session_factory

    def __enter__(self):
        self.session = self.session_factory()  # type: Session
        self.records = repository.SqlAlchemyRepository(self.session)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)
        self.session.close()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()


class YamlUnitOfWork(AbstractUnitOfWork):
    def __init__(self, yaml_file: str = "dump.yml"):
        self.yaml_database = YamlDataBase(yaml_file)

    def __enter__(self):
        self.records = repository.DictRepository(self.yaml_database.load())
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)

    def commit(self):
        self.yaml_database.dump(self.records.list())

    def rollback(self):
        pass
