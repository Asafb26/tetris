import os
from datetime import timedelta
from typing import Type

from temporalio import workflow
from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy, SearchAttributes
from temporalio.types import ParamType, ReturnType, CallableAsyncSingleParam
from temporalio.workflow import ParentClosePolicy

from core.converter import pydantic_data_converter
from core.workflow_definition import WorkflowDefinition

WORKER_QUEUE = os.getenv("WORKER_QUEUE", "default")


class BrokerClient:
    _client: Client = None

    @classmethod
    async def client(cls) -> Client:
        if cls._client is None:
            cls._client = await Client.connect("localhost:7233", data_converter=pydantic_data_converter)
        return cls._client

    @staticmethod
    async def run_activity(activity: CallableAsyncSingleParam[ParamType, ReturnType],
                           param: ParamType,
                           local: bool = False) -> ReturnType:
        if local:
            return await workflow.execute_local_activity(activity, param,
                                                         start_to_close_timeout=timedelta(seconds=60))

        return await workflow.execute_activity(activity, param, task_queue=WORKER_QUEUE,
                                               start_to_close_timeout=timedelta(seconds=60))

    @classmethod
    async def run_child_workflow(cls, definition: Type[WorkflowDefinition],
                                 arg: ParamType,
                                 parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
                                 id_reuse_policy: WorkflowIDReusePolicy = WorkflowIDReusePolicy.ALLOW_DUPLICATE,
                                 workflow_id: str | None = None,
                                 search_attributes: SearchAttributes | None = None,
                                 **workflow_identifiers) -> ReturnType:
        queue = definition.queue
        workflow_name = definition.name()
        workflow_id = workflow_id or definition.generate_workflow_id(**workflow_identifiers)

        response: definition.response = await workflow.execute_child_workflow(
            workflow_name, arg,
            id=workflow_id, task_queue=queue,
            result_type=definition.response,
            parent_close_policy=parent_close_policy,
            id_reuse_policy=id_reuse_policy,
            search_attributes=search_attributes
        )

        return response
