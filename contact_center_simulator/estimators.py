# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=invalid-name


import json
from typing import List, Dict
from abc import ABC, abstractmethod
import requests
from requests.exceptions import ConnectionError
from loguru import logger


from .types import (

    QueueItem,
    WorkItem,

    # Events

    CallEvent,
    ChurnEvent,
    ServiceStatusChangeEvent,
    TimerEvent,
    FinishEvent,
    ToAgentEvent,
    ToQueueEvent,
    FromQueueToAgentEvent
)


class Estimator(ABC):

    # def __init__(self):
    #     ...

    def handle_timer_event(self, event: TimerEvent) -> None:
        ...

    def handle_call_event(self, event: CallEvent) -> None:
        ...

    def handle_to_agent_event(self, event: ToAgentEvent) -> None:
        ...

    @abstractmethod
    def handle_to_queue_event(
        self,
        event: ToQueueEvent,
        queue: List[QueueItem],
        agents: Dict[str, WorkItem]) -> float:

        ...

    def handle_from_queue_to_agent_event(self, event: FromQueueToAgentEvent) -> None:
        ...

    def handle_finish_event(self, event: FinishEvent) -> None:
        ...

    def handle_queue_churn_event(self, event: ChurnEvent) -> None:
        ...

    def handle_service_status_change_event(self, event: ServiceStatusChangeEvent) -> None:
        ...


class EWT0(Estimator):

    name = 'EWT0'
    start_period_15 = 0
    aht_timer_period = 15 * 60  # Периодичность обновления AHT в секундах
    default_aht = 5 * 60  # Время обработки взаимодействия в секундах

    stack: dict
    service_ids: List[str]
    ahts: Dict[str, float]


    def __init__(self, service_ids: List[str]):

        self.service_ids = service_ids
        self.stack = {sid: {} for sid in self.service_ids}
        self.ahts = {sid: self.default_aht for sid in self.service_ids}

        super().__init__()


    def handle_timer_event(self, event: TimerEvent) -> None:

        # Пересчет AHT

        if event.time_handle - self.start_period_15 > self.aht_timer_period:
            for sid in self.service_ids:

                # Расчет времени обработки заявок, завершенных в периоде

                times = [y['finish_handle_time'] - y['start_handle_time']
                            for y in self.stack[sid].values()
                            if 'finish_handle_time' in y]

                if len(times) != 0:
                    self.ahts[sid] = sum(times) / len(times)
                    self.stack[sid] = {k: v for k, v in self.stack[sid].items()
                                       if 'finish_handle_time' not in v}

            self.start_period_15 = event.time_handle


    def handle_to_agent_event(self, event: ToAgentEvent) -> None:

        # Добавление события в стек для последующего расчета AHT

        self.stack[event.service_id].update({
            event.interaction_id: {'start_handle_time': event.time_handle}})


    def handle_to_queue_event(
        self,
        event: ToQueueEvent,
        queue: List[QueueItem],
        agents: Dict[str, WorkItem]) -> float:

        iid_wwt = [(q.interaction_id, (event.time_handle - q.start_queue_time) * q.weight)
              for q in queue if q.service_id == event.service_id]

        sorted_iid_wwt = sorted(iid_wwt, key=lambda x: x[1], reverse=True)
        ids, _ = list(zip(*sorted_iid_wwt))
        i_current = ids.index(event.interaction_id)
        m = len([x for x in agents.values() if x.service_id == event.service_id])
        ewt = self.ahts[event.service_id] * (i_current + 1) / m if m != 0 else -1

        return ewt


    def handle_from_queue_to_agent_event(self, event: FromQueueToAgentEvent) -> None:

        self.stack[event.service_id].update({
            event.interaction_id: {'start_handle_time': event.time_handle}})


    def handle_finish_event(self, event: FinishEvent) -> None:

        self.stack[event.service_id][event.interaction_id].update({
            'finish_handle_time': event.time_handle})


class Phantom(Estimator):

    name = 'Phantom'
    SIMULATOR_URL = 'http://localhost:8001'

    service_ids: List[str]


    def __init__(self, service_ids: List[str]):

        self.service_ids = service_ids


    def handle_to_agent_event(self, event: ToAgentEvent) -> None:

        # Отправка события в симулятор

        try:

            response = requests.post(
                self.SIMULATOR_URL + '/to_agent',
                json={
                    "timestamp": event.time_handle,
                    "agent_id": event.agent_id,
                    "interaction_id": event.interaction_id,
                    "service_id": event.service_id,
                    "weight": event.weight
                }, timeout=10
            )

            logger.debug(f'Simulator response: {event.event_type} {response.text}')

        except ConnectionError as e:

            logger.warning(f'Simulator disabled: {e}')


    def handle_to_queue_event(
        self,
        event: ToQueueEvent,
        queue: List[QueueItem],
        agents: Dict[str, WorkItem]) -> float:

        try:

            response = requests.post(self.SIMULATOR_URL + '/to_queue', json={
                "timestamp": event.time_handle,
                "interaction_id": event.interaction_id,
                "service_id": event.service_id,
                "weight": event.weight}, timeout=10)

            logger.debug(f'Simulator (report): {event.event_type} {response.text}')

            # Получение времени ожидания

            queue_item_response = requests.post(self.SIMULATOR_URL + '/get_queue_item_data', json={
                "timestamp": event.time_handle,
                "interaction_id": event.interaction_id,
                "quantile": 0.7}, timeout=10)

            logger.debug(f'Simulator (get EWT): {event.event_type} {queue_item_response.text}')

            queue_item_data = json.loads(queue_item_response.text)
            ewt = queue_item_data['predicted_waiting_time']

            logger.debug(f'Simulator EWT: {ewt}')

            return ewt

        except ConnectionError as e:

            logger.warning(f'Simulator disabled: {e}')

            return 0.0


    def handle_from_queue_to_agent_event(self, event: FromQueueToAgentEvent) -> None:

        try:

            response = requests.post(self.SIMULATOR_URL + '/from_queue_to_agent', json={
                "timestamp": event.time_handle,
                "interaction_id": event.interaction_id,
                "agent_id": event.agent_id}, timeout=10)
            logger.debug(f'Simulator response: {event.event_type} {response.text}')

        except ConnectionError as e:

            logger.warning(f'Simulator disabled: {e}')


    def handle_finish_event(self, event: FinishEvent) -> None:

        try:

            response = requests.post(self.SIMULATOR_URL + '/finish', json={
                "timestamp": event.time_handle,
                "agent_id": event.agent_id}, timeout=10)
            logger.debug(f'Simulator response: {event.event_type} {response.text}')

        except ConnectionError as e:

            logger.warning(f'Simulator disabled: {e}')


    def handle_queue_churn_event(self, event: ChurnEvent) -> None:

        try:

            response = requests.post(self.SIMULATOR_URL + '/churn', json={
                "timestamp": event.time_handle,
                "interaction_id": event.interaction_id}, timeout=10)
            logger.debug(f'Simulator response: {event.event_type} {response.text}')

        except ConnectionError as e:

            logger.warning(f'Simulator disabled: {e}')


class LES(Estimator):

    name = 'LES'
    default_ewt = 30  # Время ожидания по умолчанию в секундах

    stack: dict
    start_stack: dict


    def __init__(self):

        self.stack = {}
        self.start_stack = {}


    def handle_to_queue_event(
        self,
        event: ToQueueEvent,
        queue: List[QueueItem],
        agents: Dict[str, WorkItem]) -> float:

        self.start_stack[event.interaction_id] = event.time_handle

        try:

            ewt = self.stack[event.service_id][event.weight]

            return ewt

        except KeyError:

            return self.default_ewt


    def handle_from_queue_to_agent_event(self, event: FromQueueToAgentEvent) -> None:

        waiting_time = event.time_handle - self.start_stack[event.interaction_id]

        del self.start_stack[event.interaction_id]

        if event.service_id in self.stack:
            self.stack[event.service_id].update({event.weight: waiting_time})
        else:
            self.stack[event.service_id] = {
                event.weight: waiting_time
            }


class AverageLES(Estimator):

    name = 'AverageLES'
    default_ewt = 30  # Время ожидания по умолчанию в секундах

    max_n: int  # Число последних событий в каждую очередь, по которым берется среднее
    stack: dict
    start_stack: dict


    def __init__(self, n: int = 10):

        self.stack = {}
        self.start_stack = {}
        self.max_n = n


    def handle_to_queue_event(
        self,
        event: ToQueueEvent,
        queue: List[QueueItem],
        agents: Dict[str, WorkItem]) -> float:

        self.start_stack[event.interaction_id] = event.time_handle

        try:

            last_wt_list = self.stack[event.service_id][event.weight]

            if len(last_wt_list) > 0:
                return sum(last_wt_list) / len(last_wt_list)

            return self.default_ewt

        except KeyError:

            return self.default_ewt


    def handle_from_queue_to_agent_event(self, event: FromQueueToAgentEvent) -> None:

        waiting_time = event.time_handle - self.start_stack[event.interaction_id]

        del self.start_stack[event.interaction_id]

        if event.service_id in self.stack:
            if event.weight in self.stack[event.service_id]:
                self.stack[event.service_id][event.weight].append(waiting_time)
            else:
                self.stack[event.service_id][event.weight] = [waiting_time]
        else:
            self.stack[event.service_id] = {
                event.weight: [waiting_time]
            }

        if len(self.stack[event.service_id][event.weight]) > self.max_n:
            self.stack[event.service_id][event.weight] = \
                self.stack[event.service_id][event.weight][-self.max_n:]


class P_LES(Estimator):

    name = 'P-LES'
    default_ewt = 30  # Время ожидания по умолчанию в секундах

    stack: dict
    start_stack: dict


    def __init__(self):

        self.start_stack = {}
        self.stack = {}


    def handle_to_queue_event(
        self,
        event: ToQueueEvent,
        queue: List[QueueItem],
        agents: Dict[str, WorkItem]) -> float:

        self.start_stack[event.interaction_id] = (event.time_handle, len(queue))

        try:

            wt, pos = self.stack[event.service_id][event.weight]

            new_pos = len(queue)

            ewt = new_pos * wt / pos

            return ewt

        except KeyError:

            return self.default_ewt


    def handle_from_queue_to_agent_event(self, event: FromQueueToAgentEvent) -> None:

        waiting_time = event.time_handle - self.start_stack[event.interaction_id][0]
        position = self.start_stack[event.interaction_id][1]

        del self.start_stack[event.interaction_id]

        if event.service_id in self.stack:
            self.stack[event.service_id].update({event.weight: (waiting_time, position)})
        else:
            self.stack[event.service_id] = {
                event.weight: (waiting_time, position)
            }


class E_LES(Estimator):

    name = 'E-LES'
    default_ewt = 30  # Время ожидания по умолчанию в секундах

    k: float

    def __init__(self, k: float = 1.0):

        self.k = k


    def handle_to_queue_event(
        self,
        event: ToQueueEvent,
        queue: List[QueueItem],
        agents: Dict[str, WorkItem]) -> float:

        try:

            wts = [obj.start_queue_time for obj in queue
                  if obj.service_id == event.service_id and obj.weight ==event.weight]

            if len(wts) > 0:
                ewt = self.k * (event.time_handle - wts[0])
                return ewt

            return self.default_ewt

        except KeyError:

            return self.default_ewt
