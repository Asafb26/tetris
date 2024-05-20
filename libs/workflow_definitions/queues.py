from enum import StrEnum


class Queues(StrEnum):
    ORCHESTRATOR = 'orchestrator_queue'
    COLLECTORS = 'collectors_queue'
