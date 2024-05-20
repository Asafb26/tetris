import asyncio

from temporalio.client import Client
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from core.converter import pydantic_data_converter
from libs.data_types.platform import Platform
from microservices.collectors.handlers.collect.collect_workflow import CollectTask
from libs.workflows.collect_orchestration.workflow import CollectOrchestrationWorkflow


async def main():
    # Create client connected to server at the given address
    client = await Client.connect("localhost:7233", data_converter=pydantic_data_converter)

    #
    # await client.create_schedule(
    #     "workflow-schedule-id",
    #     Schedule(
    #         action=ScheduleActionStartWorkflow(
    #             YourSchedulesWorkflow.run,
    #             "my schedule arg",
    #             id="schedules-workflow-id",
    #             task_queue="schedules-task-queue",
    #         ),
    #         spec=ScheduleSpec(
    #             intervals=[ScheduleIntervalSpec(every=timedelta(minutes=2))]
    #         ),
    #         state=ScheduleState(note="Here's a note on my Schedule."),
    #     ),
    # )

    org = [
        {
            "platform": Platform.ETHEREUM,
            "account": "0x1234",
        },
        {
            "platform": Platform.BINANCE,
            "account": "1234",
        },
    ]

    account = org[0]
    task = CollectTask(platform=account["platform"], wallet=account["account"])

    # Execute a workflow
    try:
        # handle = await client.start_workflow(
        #     CollectOrchestrationWorkflow.run, task,
        #     id=f'collect-{task.platform}-{task.wallet}', task_queue="core_queue",
        #     id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        # )
        handle = await client.start_workflow(
            CollectOrchestrationWorkflow.run,
            id=f'collect-orch', task_queue="core_queue",
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE
        )
    except WorkflowAlreadyStartedError as e:
        print(f"Workflow already started: {e}")
        return

    # print(f"Result: {await handle.result()}")


if __name__ == "__main__":
    asyncio.run(main())
