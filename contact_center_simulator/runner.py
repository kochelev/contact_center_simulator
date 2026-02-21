# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=invalid-name
# pylint: disable=line-too-long
# pylint: disable=import-outside-toplevel
# pylint: disable=relative-beyond-top-level

import json
from copy import deepcopy
from datetime import datetime
from typing import Optional
from urllib.error import URLError
from prometheus_client import push_to_gateway
import nest_asyncio  # Эта строчка нужна только в Jupyter

from .generator import Generator
from .types import (
    TimerEvent,
    CallEvent,
    ToAgentEvent,
    ToQueueEvent,
    ChurnEvent,
    FromQueueToAgentEvent,
    FinishEvent,
    ServiceStatusChangeEvent,
    QueueItem,
    WorkItem
)

from .metrics import (
    registry,

    SCPL_ISSUES,
    SCPL_QUEUING,
    SCPL_INCOME,
    SCPL_CHURN,
    SCPL_HANDLING_TIME,
    SCPL_WAITING_TIME,
    SCPL_WAITING_TIME_ERROR
)

nest_asyncio.apply() # Эта строчка нужна только в Jupyter


class MyMetrics:

    GATEWAY_URL = 'localhost:9091'

    def push(self):
        try:
            push_to_gateway(self.GATEWAY_URL, job='runner', registry=registry)
        except URLError:
            pass


def event_logging(fn):

    def event_logger(self, e, *args, **kwargs):

        # Предшествующие шаги

        d = {'event_type': e.__class__.__name__}
        d.update(e.__dict__)

        if d['event_type'] == 'TimerEvent':
            # Отображать события таймера в логах требуется только при детальной трассировке
            self.lgr.trace(f'FUNC_START\t{str(json.dumps(d))}')
        else:
            self.lgr.debug(f'FUNC_START\t{str(json.dumps(d))}')

        # Выполнение функции

        result = fn(self, e, *args, **kwargs)

        # Последующие шаги

        if d['event_type'] == 'TimerEvent':
            # Отображать события таймера в логах требуется только при детальной трассировке
            self.lgr.trace(f'FUNC_FINISH\t{str(json.dumps(d))}')
        else:
            self.lgr.info(f'FUNC_FINISH\t{str(json.dumps(d))}')

        # if d['event_type'] == 'ToQueueEvent':
        #     self.lgr.info(f'TO_QUEUE: {e.time_handle} | ' + \
        #     f'iid: {e.interaction_id} | queue_position: {len(self.GEN.state.queue) + 1}')

        return result
    return event_logger


class Runner:

    actions: dict
    last_income_moments: dict
    ewt_statistics: dict
    shared_params: dict
    GEN: Generator

    def __init__(
        self,
        methods,
        general_timer: int,
        services_statuses_recount_timer: int,
        ewt_estimators: list,
        logger):

        self.methods = methods
        self.general_timer = general_timer
        self.services_statuses_recount_timer = services_statuses_recount_timer
        self.ewt_estimators = ewt_estimators

        self.lgr = logger
        self.metrics = MyMetrics()


    def run(
        self,
        shared_params,
        time_scale: float,
        number_of_calls: Optional[int]):

        self.shared_params = shared_params

        self.GEN = Generator(
            methods=self.methods,
            general_timer=self.general_timer,
            services_statuses_recount_timer=self.services_statuses_recount_timer)

        x = self.GEN.events_iterator(
            shared_params=shared_params,
            time_scale=time_scale,
            number_of_calls=number_of_calls)

        self.last_income_moments = {sid: 0.0 for sid in shared_params['service_ids']}
        self.ewt_statistics = {}

        actions = {
            'TIMER': self.__timer,
            'CALL': self.__call,
            'TO_AGENT': self.__to_agent,
            'TO_QUEUE': self.__to_queue,
            'FROM_QUEUE_TO_AGENT': self.__from_queue_to_agent,
            'FINISH': self.__finish,
            'CHURN': self.__churn,
            'SERVICE_STATUS_CHANGE': self.__service_status_change
        }

        while True:
            try:
                event = next(x)
                actions[event.event_type](event)
            except StopIteration:
                break


    @event_logging
    def __timer(self, event: TimerEvent):

        for ee in self.ewt_estimators:
            ee.handle_timer_event(event)

    @event_logging
    def __call(self, event: CallEvent):

        for ee in self.ewt_estimators:
            ee.handle_call_event(event)

        SCPL_INCOME.labels(sid=event.service_id, kind='generated').set(
            event.time_handle - self.last_income_moments[event.service_id])
        SCPL_ISSUES.labels(sid=event.service_id, kind='calls').inc()
        self.metrics.push()

        self.last_income_moments[event.service_id] = event.time_handle

    @event_logging
    def __to_agent(self, event: ToAgentEvent):

        for ee in self.ewt_estimators:
            ee.handle_to_agent_event(event)

    @event_logging
    def __to_queue(self, event: ToQueueEvent):

        SCPL_QUEUING.labels(sid=event.service_id).set(
            sum(1 for e in self.GEN.state.queue if e.service_id == event.service_id))
        self.metrics.push()

        Q = [QueueItem(
                interaction_id=q.id,
                service_id=q.service_id,
                service_index=self.shared_params['service_ids'].index(q.service_id),
                weight=q.weight,
                start_queue_time=q.time_income
            ) for q in self.GEN.state.queue]

        Z = {agent_id: WorkItem(
                agent_id=agent_id,
                agent_index=self.shared_params['agent_ids'].index(agent_id),
                interaction_id=self.GEN.state.agents[agent_id].interaction_data.id,
                service_id=self.GEN.state.agents[agent_id].interaction_data.service_id,
                service_index=self.shared_params['service_ids'].index(self.GEN.state.agents[agent_id].interaction_data.service_id),
                start_handle_time=self.GEN.state.agents[agent_id].busy_from_time
            ) for agent_id in self.shared_params['agent_ids']
                if self.GEN.state.agents[agent_id].interaction_data is not None}

        ewts = {}

        for ee in self.ewt_estimators:

            ewt = ee.handle_to_queue_event(
                event=event,
                queue=Q,
                agents=Z
            )

            if ewt is None:
                continue

            ewts[ee.name] = ewt

        self.ewt_statistics.update({event.interaction_id: {
            'queue_time': event.time_handle,
            'service_id': event.service_id,
            'weight': event.weight,
            'ewts': ewts
        }})

        # self.lgr.info(f'TO_QUEUE: {event.time_handle} | ' + \
        #     f'iid: {event.interaction_id} | queue_position: {len(self.GEN.state.queue) + 1}')

        # self.lgr.log('DATA', f'{str(ewt_statistics)}')

    @event_logging
    def __from_queue_to_agent(self, event: FromQueueToAgentEvent):

        SCPL_QUEUING.labels(sid=event.service_id).set(
            sum(1 for e in self.GEN.state.queue if e.service_id == event.service_id))
        self.metrics.push()

        for ee in self.ewt_estimators:
            ee.handle_from_queue_to_agent_event(event)

        new_data = deepcopy(self.ewt_statistics[event.interaction_id])
        new_data.update({
            'distribution_time': event.time_handle,
            'queuing_time': event.time_handle - new_data['queue_time']})
        self.ewt_statistics.update({event.interaction_id: new_data})

        interaction_data = self.ewt_statistics[event.interaction_id]

        SCPL_WAITING_TIME.labels(
            sid=event.service_id,
            weight=interaction_data['weight'],
            kind='real').set(new_data['queuing_time'])

        for ewt_name, ewt_value in interaction_data['ewts'].items():
            SCPL_WAITING_TIME.labels(
                sid=event.service_id,
                weight=interaction_data['weight'],
                kind=ewt_name).set(ewt_value)
            SCPL_WAITING_TIME_ERROR.labels(
                sid=event.service_id,
                weight=interaction_data['weight'],
                kind=ewt_name).set(ewt_value - new_data['queuing_time'])
        self.metrics.push()

        # self.lgr.log('DATA', f'{str(self.ewt_statistics)}')

    @event_logging
    def __finish(self, event: FinishEvent):

        for ee in self.ewt_estimators:
            ee.handle_finish_event(event)

        SCPL_HANDLING_TIME.labels(
            aid=event.agent_id,
            sid=event.service_id,
            kind='generated').set(event.handling_time)
        SCPL_ISSUES.labels(
            sid=event.service_id,
            kind='finished').inc()
        self.metrics.push()

    @event_logging
    def __churn(self, event: ChurnEvent):

        for ee in self.ewt_estimators:
            ee.handle_queue_churn_event(event)

        SCPL_CHURN.labels(sid=event.service_id, kind='generated').set(event.waiting_time)
        SCPL_ISSUES.labels(sid=event.service_id, kind='churned').inc()
        self.metrics.push()

    @event_logging
    def __service_status_change(self, event: ServiceStatusChangeEvent):

        for ee in self.ewt_estimators:
            ee.handle_service_status_change_event(event)
