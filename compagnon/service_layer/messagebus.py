from typing import Dict

from compagnon.domain import events


def handle(event: events.Event):
    for handler in HANDLERS[type(event)]:
        handler(event)


def send_out_of_stock_notification(event: events.AddRecord):
    print("Event")


HANDLERS = {
    events.AddRecord: [send_out_of_stock_notification],
    events.AddExecution: [send_out_of_stock_notification],
}  # type: Dict[Type[events.Event], List[Callable]]
