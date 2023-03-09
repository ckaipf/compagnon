import abc

import compagnon.domain.model as model


class AbstractRepository(abc.ABC):
    def __init__(self):
        self.seen = dict()  # type: Dict[str, model.Record]

    def add(self, record: model.Record):
        self._add(record)
        self.seen[record.foreign_id] = record

    def get(self, foreign_id) -> model.Record:
        record = self._get(foreign_id)
        if record:
            self.seen[record.foreign_id] = record
        return record

    @abc.abstractmethod
    def _add(self, record: model.Record):
        raise NotImplementedError

    @abc.abstractmethod
    def _get(self, foreign_id) -> model.Record:
        raise NotImplementedError

    @abc.abstractmethod
    def list(self):
        raise NotImplementedError



class DictRepository(AbstractRepository):
    def __init__(self, records):
        super().__init__()
        self._records = {r.foreign_id: r for r in records}

    def _add(self, records):
        self._records[records.foreign_id] = records

    def _get(self, foreign_id):
        return self._records[foreign_id]

    def list(self):
        return list([record for record in self._records.values()])


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session):
        super().__init__()
        self.session = session

    def _add(self, records):
        self.session.add(records)

    def _get(self, foreign_id):
        return self.session.query(model.Record).filter_by(foreign_id=foreign_id).one()

    def list(self):
        return self.session.query(model.Record).all()
