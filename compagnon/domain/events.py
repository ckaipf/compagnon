from dataclasses import dataclass


class Event:
    pass


@dataclass
class AddExecution(Event):
    pass


@dataclass
class AddRecord(Event):
    pass
