# import asyncio
#
# from core.worker import BaseWorker
# from libs.collect.activities import collect
# from libs.workflows.collect.workflow import CollectWorkflow
#
# CollectorWorker = BaseWorker(name="collector")
#
# CollectorWorker.handle_workflows(CollectWorkflow)
# CollectorWorker.handle_activities(collect)
#
#
# async def main():
#     await CollectorWorker.run()
#
#
# if __name__ == "__main__":
#     asyncio.run(main())


import asyncio
from datetime import timedelta

from core.worker import BaseWorker
from libs.workflow_definitions.queues import Queues
from microservices.orchestrator.handlers.collect_orchestration.activities.get_running_slots_activity import \
    get_running_slots
from microservices.orchestrator.handlers.collect_orchestration.activities.prepare_collect_activity import \
    prepare_collect
from microservices.orchestrator.handlers.collect_orchestration.collect_orchestration_workflow import \
    CollectOrchestrationWorkflow

OrchestratorWorker = BaseWorker(name="orchestrator")

# OrchestratorWorker.handle_old(workflow=CollectOrchestrationWorkflow,
#                           activities=[prepare_collect, get_running_slots])
#


OrchestratorWorker.handle(handler=CollectOrchestrationWorkflow,
                          activities=[prepare_collect, get_running_slots],
                          schedule=timedelta(minutes=1))


# OrchestratorWorker.handle_schedule(workflow=CollectOrchestrationWorkflow, every=timedelta(minutes=1))


async def main():
    await OrchestratorWorker.run()


if __name__ == "__main__":
    asyncio.run(main())
