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

from core.worker import BaseWorker
from microservices.collectors.handlers.collect.activities.collect_activity import collect
from microservices.collectors.handlers.collect.collect_workflow import CollectWorkflow

CollectorWorker = BaseWorker(name="collector")

CollectorWorker.handle(handler=CollectWorkflow,
                       activities=[collect])


async def main():
    await CollectorWorker.run()


if __name__ == "__main__":
    asyncio.run(main())
