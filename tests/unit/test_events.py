import datetime

import compagnon.domain.events as events
import compagnon.domain.model as model
import compagnon.service_layer.services as services
import compagnon.service_layer.unit_of_work as unit_of_work


def test_records_out_of_stock_event_if_cannot_allocate(add_one_execution):
    record = model.Record(
        foreign_id="abc", data={"x": 1}, creation_time=datetime.datetime.now()
    )
    assert record.events[-1] == events.AddRecord()
    record.add_execution(
        add_one_execution(record, creation_time=datetime.datetime.now())
    )
    assert record.events[-1] == events.AddExecution()
