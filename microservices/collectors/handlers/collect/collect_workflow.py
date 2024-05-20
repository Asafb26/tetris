from temporalio import workflow

from core.broker_client import BrokerClient
from core.worker import workflow_definition
from libs.workflow_definitions.collectors.collect_workflow import CollectDefinition
from microservices.collectors.handlers.collect.activities.collect_activity import CollectAction, collect, \
    CollectResponse, CollectResult


@workflow_definition(CollectDefinition)
class CollectWorkflow:
    _exit: bool
    _account: str

    def __init__(self) -> None:
        self._exit = False
        self._account = ""

    @workflow.signal
    def exit(self) -> None:
        print(f"Received exit signal {self._account}")
        self._exit = True

    @workflow.query
    def account(self) -> str:
        return self._account

    @workflow.run
    async def run(self, collect_task: CollectDefinition.request) -> CollectDefinition.response:
        self._account = collect_task.wallet
        num_of_tx_collected = 0

        while not self._exit:
            collect_action = CollectAction(
                platform=collect_task.platform,
                account=collect_task.wallet)

            collect_response: CollectResponse = await BrokerClient.run_activity(
                collect, collect_action
            )

            num_of_tx_collected += collect_response.num_of_tx

            if collect_response.result == CollectResult.STOP:
                print(f"Stopping collection from {collect_task.platform} {collect_task.wallet}")
                self._exit = True

        return num_of_tx_collected
