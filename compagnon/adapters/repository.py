import abc

import compagnon.domain.model as model


class AbstractRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, record: model.Record):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, foreign_id) -> model.Record:
        raise NotImplementedError

    @abc.abstractmethod
    def list(self):
        pass


class DictRepository(AbstractRepository):
    def __init__(self, records):
        self._records = {r.foreign_id: r for r in records}

    def add(self, records):
        self._records[records.foreign_id] = records

    def get(self, foreign_id):
        return self._records[foreign_id]

    def list(self):
        return list([record for record in self._records.values()])


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session):
        self.session = session

    def add(self, records):
        self.session.add(records)

    def get(self, foreign_id):
        return self.session.query(model.Record).filter_by(foreign_id=foreign_id).one()

    def list(self):
        return self.session.query(model.Record).all()
