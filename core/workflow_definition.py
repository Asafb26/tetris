from abc import ABC, abstractmethod
from typing import ClassVar

from pydantic import BaseModel


class WorkflowDefinition(BaseModel, ABC):
    request: ClassVar[BaseModel | None]
    response: ClassVar[BaseModel | None]
    queue: ClassVar[str]

    @classmethod
    def name(cls) -> str:
        return cls.__name__.lower().replace('Definition', '')

    @classmethod
    def generate_workflow_id(cls, *args, **kwargs) -> str:
        return f'{cls.name()}-{cls._workflow_identifiers(*args, **kwargs)}'

    @classmethod
    @abstractmethod
    def _workflow_identifiers(cls, *args, **kwargs) -> str:
        raise NotImplementedError
