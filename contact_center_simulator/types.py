# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring


from dataclasses import dataclass
from typing import List, Dict, Union, Optional


# ENTITIES


@dataclass
class ServiceStatusChange:

    status_before: int
    status_after: int


@dataclass
class Interaction:

    id: str
    service_id: str
    weight: float

    time_income: float
    time_churn: Union[float, None] = None
    time_handle: Union[float, None] = None
    time_finish: Union[float, None] = None


@dataclass
class Agent:

    id: str
    free_from_time: Optional[float]
    busy_from_time: Optional[float]
    interaction_data: Union[Interaction, None] = None


@dataclass
class State:

    agents: Dict[str, Agent]
    queue: List[Interaction]

    total_income: int = 0
    total_churn: int = 0
    total_finish: int = 0


@dataclass
class QueueItem():

    interaction_id: str  # идентификатор заявки
    service_id: str  # идентификатор сервиса заявки
    service_index: int  # индекс сервиса заявки
    weight: float  # приоритет (вес) заявки в 0.1 - 10 с шагом 0.1 (w < 1) и 1 (w > 1)
    start_queue_time: float  # время постановки заявки в очередь


@dataclass
class WorkItem():

    agent_id: str  # идентификатор оператора
    agent_index: int  # индекс оператора в массиве их идентификаторов
    interaction_id: str  # идентификатор заявки в обработке
    service_id: str  # идентификатор сервиса заявки
    service_index: int  # индекс сервиса заявки
    start_handle_time: float  # время взятия заявки в обработку оператором


# EVENTS


@dataclass
class TimerEvent:

    time_handle: float
    real_time: float
    event_type = 'TIMER'

    start_time: float
    timer_period: int


@dataclass
class CallEvent:

    time_handle: float
    event_type = 'CALL'

    interaction_id: str
    service_id: str
    weight: float


@dataclass
class ToAgentEvent:

    time_handle: float
    event_type = 'TO_AGENT'

    interaction_id: str
    service_id: str
    weight: float

    agent_id: str
    generated_handling_time: float


@dataclass
class ToQueueEvent:

    time_handle: float
    event_type = 'TO_QUEUE'

    interaction_id: str
    service_id: str
    weight: float

    generated_waiting_time: float


@dataclass
class FromQueueToAgentEvent:

    time_handle: float
    event_type = 'FROM_QUEUE_TO_AGENT'

    interaction_id: str
    service_id: str
    weight: float

    agent_id: str
    waiting_time: float
    generated_handling_time: float


@dataclass
class FinishEvent:

    time_handle: float
    event_type = 'FINISH'

    interaction_id: str
    service_id: str

    agent_id: str
    handling_time: float


@dataclass
class ChurnEvent:

    time_handle: float
    event_type = 'CHURN'

    interaction_id: str
    service_id: str

    waiting_time: float


@dataclass
class ServiceStatusChangeEvent:

    time_handle: float
    event_type = 'SERVICE_STATUS_CHANGE'

    service_id: str
    state_before: int
    state_after: int
