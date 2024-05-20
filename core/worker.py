import dataclasses
import os
from datetime import timedelta
from typing import Callable, Type

from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
from temporalio import workflow, activity
from temporalio.client import Client, Schedule, ScheduleActionStartWorkflow, ScheduleSpec, ScheduleIntervalSpec, \
    ScheduleAlreadyRunningError
from temporalio.types import ClassType, CallableType
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

from core.converter import pydantic_data_converter
from core.workflow_definition import WorkflowDefinition
from libs.schema.collect_status import CollectStatus
from libs.schema.raw_transaction import RawTransaction


# Due to known issues with Pydantic's use of issubclass and our inability to
# override the check in sandbox, Pydantic will think datetime is actually date
# in the sandbox. At the expense of protecting against datetime.now() use in
# workflows, we're going to remove datetime module restrictions. See sdk-python
# README's discussion of known sandbox issues for more details.
def new_sandbox_runner() -> SandboxedWorkflowRunner:
    return SandboxedWorkflowRunner(
        restrictions=dataclasses.replace(
            SandboxRestrictions.default.with_passthrough_modules("beanie"),
            invalid_module_members=dataclasses.replace(
                SandboxRestrictions.invalid_module_members_default.with_child_unrestricted('datetime'),
            ),
        )
    )


def workflow_definition(handler_type: Type[WorkflowDefinition]):
    def decorator(cls: ClassType) -> ClassType:
        if not callable(getattr(cls, 'run', None)):
            raise ValueError(f"Handler {cls.__name__} is missing a run method")

        cls.run = workflow.run(cls.run)
        return workflow.defn(name=handler_type.name())(cls)

    return decorator


def workflow_activity(fn: CallableType):
    activity.defn(fn)

    return fn


class BaseWorker:
    activities: list[Callable]
    workflows: list[Type]
    schedules: list[dict]
    queue: str

    def __init__(self, name: str):
        self.name = name
        self.queue = os.getenv("WORKER_QUEUE")
        self.activities = []
        self.workflows = []
        self.schedules = []

    # def handle_old(self,
    #                workflow: Type | None = None,
    #                activities: list[Callable] | None = None):
    #     if workflow:
    #         self.workflows.append(workflow)
    #     if activities:
    #         self.activities.extend(activities)

    def handle(self,
               handler: Type[WorkflowDefinition],
               activities: list[Callable] | None = None,
               schedule: timedelta | None = None):

        self.workflows.append(handler)

        if activities:
            self.activities.extend(activities)

        if schedule:
            self.schedules.append(dict(workflow=handler, every=schedule))

    def handle_schedule(self,
                        workflow: Type,
                        every: timedelta):
        self.schedules.append(dict(workflow=workflow, every=every))

    async def run(self):
        mongo_client = AsyncIOMotorClient("mongodb://root:example@127.0.0.1:27017")

        await init_beanie(database=mongo_client.db_name, document_models=[RawTransaction, CollectStatus])

        temporal_client = await Client.connect("localhost:7233", namespace="default",
                                               data_converter=pydantic_data_converter)

        for schedule in self.schedules:
            workflow = schedule['workflow']
            every = schedule['every']

            try:
                await temporal_client.create_schedule(
                    f'schedule-{workflow.__name__}',
                    Schedule(
                        action=ScheduleActionStartWorkflow(
                            workflow.run,
                            id=f"schedules-workflow-{workflow.__name__}",
                            task_queue=self.queue,
                        ),
                        spec=ScheduleSpec(
                            intervals=[ScheduleIntervalSpec(every=every)]
                        )
                    ),
                )
            except ScheduleAlreadyRunningError as e:
                continue

        # Run a worker for the workflow
        worker = Worker(
            temporal_client,
            identity=self.name,
            task_queue=self.queue,
            workflows=self.workflows,
            activities=self.activities,
            workflow_runner=new_sandbox_runner(),
        )

        await worker.run()
