from datetime import timedelta, datetime

from pydantic import BaseModel
from temporalio import workflow
from temporalio.common import WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError

from core.broker_client import BrokerClient
from core.worker import workflow_definition
from libs.data_types.platform import Platform
from libs.workflow_definitions.collectors.collect_workflow import CollectDefinition
from libs.workflow_definitions.orchestrator.collect_orchestration_workflow import CollectOrchestrationDefinition
from microservices.orchestrator.handlers.collect_orchestration.activities.get_running_slots_activity import RunningSlot, \
    GetRunningSlotsAction, GetRunningSlotsResponse, get_running_slots
from microservices.orchestrator.handlers.collect_orchestration.activities.prepare_collect_activity import \
    prepare_collect, PrepareCollectAction, AccountToCollect, PrepareCollectResponse

collectors = {
    Platform.ETHEREUM: ['1', '2', '3'],
    Platform.BINANCE: ['1', '2'],
}

COOLDOWN_HOURS = 2


class Slot(BaseModel):
    id: int
    api_key: str
    is_running: bool


@workflow_definition(CollectOrchestrationDefinition)
class CollectOrchestrationWorkflow:
    def __init__(self):
        self._cooldown = timedelta(hours=COOLDOWN_HOURS)

    async def run(self) -> CollectOrchestrationDefinition.response:
        print("**** Starting CollectOrchestrationWorkflow")
        for platform, platform_configs in collectors.items():
            num_of_slots = len(platform_configs)
            prepare_collect_action = PrepareCollectAction(
                platform=platform,
                num_of_slots=num_of_slots
            )

            print(f"**** Preparing collection from {platform}")

            prepare_collect_response: PrepareCollectResponse = await BrokerClient.run_activity(
                prepare_collect, prepare_collect_action
            )

            print(f"**** Current time: {prepare_collect_response.current_time}")

            if not prepare_collect_response.accounts_to_collect:
                print(f"**** No accounts to collect from {platform}")
                continue

            get_running_slots_action = GetRunningSlotsAction(
                platform=platform
            )

            running_slots_response: GetRunningSlotsResponse = await BrokerClient.run_activity(
                get_running_slots, get_running_slots_action
            )

            print(f"**** Number of running slots: {len(running_slots_response.slots)}/ {num_of_slots}")
            topology = self._build_topology(platform_configs=platform_configs,
                                            running_slots=running_slots_response.slots)

            print(f"**** Topology: {topology}")

            for account_to_collect in prepare_collect_response.accounts_to_collect:
                if self._is_in_cooldown(account_to_collect, prepare_collect_response.current_time):
                    print(f"**** Skipping collection from {platform} {account_to_collect.account}")
                    continue

                chosen_slot = topology.pop(0)
                workflow_id = CollectDefinition.generate_workflow_id(platform=platform, slot_id=chosen_slot.id)
                if chosen_slot.is_running:
                    print(f"**** Slot {chosen_slot.id} is running, stopping it")
                    handle = workflow.get_external_workflow_handle(workflow_id)
                    await handle.signal('exit')
                    continue

                print(f"**** Starting collection from {platform} {account_to_collect.account}")
                collect_task = CollectDefinition.request(
                    platform=platform,
                    wallet=account_to_collect.account
                )

                try:
                    await BrokerClient.run_child_workflow(
                        CollectDefinition, collect_task,
                        workflow_id=workflow_id,
                        parent_close_policy=workflow.ParentClosePolicy.ABANDON,
                        id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
                        search_attributes={'Platform': [platform], 'Account': [collect_task.wallet]}
                    )
                except WorkflowAlreadyStartedError as e:
                    print(f"**** Workflow already started: {e}")

    def _build_topology(self, platform_configs: list[str], running_slots: list[RunningSlot]) -> list[Slot]:
        slots = []
        running_slot_ids = [slot.id for slot in running_slots]

        for slot_id, api_key in enumerate(platform_configs):
            if slot_id in running_slot_ids:
                continue

            slots.append(Slot(
                id=slot_id,
                api_key=api_key,
                is_running=False
            ))

        for slot in sorted(running_slots, key=lambda x: x.start_time):
            if slot.id >= len(platform_configs):
                print(f"**** Slot {slot.id} is not in platform configs")
                continue

            slots.append(Slot(
                id=slot.id,
                api_key=platform_configs[slot.id],
                is_running=True
            ))

        return slots

    def _is_in_cooldown(self, account_to_collect: AccountToCollect, current_time: datetime) -> bool:
        return account_to_collect.last_synced_at + self._cooldown > current_time
