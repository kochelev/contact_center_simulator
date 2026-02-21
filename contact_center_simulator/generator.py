# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

import time
import uuid
import copy
from datetime import datetime, timezone
from typing import Optional, List, TypeVar
from deepdiff import DeepDiff
from .types import (

    Interaction,
    Agent,
    State,

    # Events

    CallEvent,
    ToAgentEvent,
    ToQueueEvent,
    FromQueueToAgentEvent,
    FinishEvent,
    ChurnEvent,
    ServiceStatusChangeEvent,
    TimerEvent
)

from .configurations import Configuration
M = TypeVar('M', bound=Configuration)

# TODO: посмотреть в сторону Protocol вместе TypeVar


class Generator():

    timer_period: int

    methods: M
    events_to_handle: List = []
    last_service_intensities = None
    state: State

    def __init__(
        self,
        # Методы генерации интервалов и др.
        methods: Configuration,
        # Интервал срабатывания события TIMER в секундах
        general_timer: int,
        # Интервал пересчета порогов по таймеру в секундах
        services_statuses_recount_timer: Optional[int]):


        self.methods = methods
        self.timer_period = general_timer
        self.services_statuses_recount_timer = \
            services_statuses_recount_timer


    def events_iterator(
        self,
        shared_params,
        time_scale: float,
        number_of_calls: Optional[int]):

        assert time_scale >= 0, \
            "Масштаб времени не может быть отрицательным"

        assert number_of_calls is None or number_of_calls > 0, \
            "Число входящих звонков должно быть больше 0 или None"

        assert time_scale > 0.000001 or number_of_calls is not None, \
            "При нулевом масштабе времени необходимо задать число звонков"

        self.state = State(
            agents={
                agent_id: Agent(id=agent_id,
                                free_from_time=0,
                                busy_from_time=None,
                                interaction_data=None)
                for agent_id in shared_params['agent_ids']}, # Справочник операторов

            queue=[], # Очередь ожидания ответа оператора
        )

        self.last_service_intensities = copy.deepcopy(shared_params['services_intensities'])

        events_to_handle = []
        last_handled_time = 0
        start_datetime = datetime.now().timestamp()

        while True:

            # Первое событие в стек, запуск цикла

            if len(events_to_handle) == 0:

                # Данное условие позволяет закончить цикл,
                # если событий больше нет и не будет

                if number_of_calls == 0:
                    break

                events_to_handle.append({
                    'time_handle': last_handled_time + self.timer_period,
                    'event_type': 'timer'
                })

                for service_id in shared_params['service_ids']:

                    if number_of_calls == 0:
                        break

                    if number_of_calls is not None:
                        number_of_calls -= 1

                    delay = self.methods.events_delay(shared_params, service_id)
                    weight = self.methods.interaction_weight(shared_params, service_id)

                    if delay is not None:
                        events_to_handle.append({
                            'time_handle': delay,
                            'event_type': 'incoming_event',
                            'service_id': service_id,
                            'interaction_id': str(uuid.uuid4()),
                            'weight': weight
                        })

                events_to_handle = sorted(events_to_handle,
                                          key=lambda x: x['time_handle'])

                continue

            # Блок для обновления измененных интенсивностей сервисов.
            # Данный блок необходим, так как возможна ситуация, когда при низкой
            # интенсивности входящих звонков до следующего слишком много времени,
            # и увеличение интенсивности вступит в силу, только когда оно наступит.

            for service_id, params in shared_params['services_intensities'].items():

                # Отбор сервисов, у которых изменилась интенсивность

                diff = DeepDiff(
                    params,
                    self.last_service_intensities[service_id])

                if 'values_changed' in diff:

                    # Сохранить обновленную интенсивность сервиса

                    self.last_service_intensities[service_id] = \
                        copy.deepcopy(shared_params['services_intensities'][service_id])

                    # Поиск в очереди предстоящих событий входящих звонков
                    # по сервису, у которого изменилась интенсивность

                    indecies_to_delete = []

                    for i, event in enumerate(events_to_handle):
                        if event['event_type'] == 'incoming_event' and \
                           event['service_id'] == service_id:

                            indecies_to_delete.append(i)
                            break

                    # Удаление из очереди событий входящих звонков
                    # по сервису, у которого изменилась интенсивность

                    for i in indecies_to_delete:
                        del events_to_handle[i]

                    # Сгенерировать новое событие входящего звонка

                    delay = self.methods.events_delay(shared_params, service_id)
                    weight = self.methods.interaction_weight(shared_params, service_id)

                    if delay is not None:
                        events_to_handle.append({
                            'time_handle': last_handled_time + delay,
                            'event_type': 'incoming_event',
                            'service_id': service_id,
                            'interaction_id': str(uuid.uuid4()),
                            'weight': weight
                        })

                events_to_handle = sorted(
                    events_to_handle,
                    key=lambda x: x['time_handle'])

                continue

            # Обработка событий из стека

            event = events_to_handle.pop(0)

            if time_scale > 0:
                time.sleep((event['time_handle'] - last_handled_time) * time_scale)

            last_handled_time = event['time_handle']

            # Событие = сработал таймер каждые X секунд

            if event['event_type'] == 'timer':

                if len(events_to_handle) == 0:
                    break

                events_to_handle.append({
                    'time_handle': event['time_handle'] + self.timer_period,
                    'event_type': 'timer'
                })

                yield TimerEvent(
                    time_handle=event['time_handle'],
                    real_time=datetime.now(tz=timezone.utc).timestamp(),
                    start_time=start_datetime,
                    timer_period=self.timer_period
                )

                events_to_handle = sorted(events_to_handle,
                                               key=lambda x: x['time_handle'])

                continue

            # Событие = пересчет загузки сервисов

            if event['event_type'] == 'services_statuses_recount':

                changes = self.methods.services_statuses_recount_on_timer(
                    shared_params,
                    self.state.queue,
                    event['time_handle'])

                events_to_handle.append({
                    'time_handle': event['time_handle'] + self.services_statuses_recount_timer,
                    'event_type': 'services_statuses_recount'
                })

                if changes is not None:
                    for service_id, service_changes in changes.items():
                        yield ServiceStatusChangeEvent(
                            time_handle=event['time_handle'],
                            service_id=service_id,
                            state_before=service_changes.status_before,
                            state_after=service_changes.status_after
                        )

                events_to_handle = sorted(
                    events_to_handle,
                    key=lambda x: x['time_handle'])

                continue

            # Событие = входящий звонок

            if event['event_type'] == 'incoming_event':

                self.state.total_income += 1

                yield CallEvent(
                    time_handle=event['time_handle'],
                    interaction_id=event['interaction_id'],
                    service_id=event['service_id'],
                    weight=event['weight']
                )

                # Сразу генерируется следующее входящее событие в список событий

                if number_of_calls is None or number_of_calls > 0:

                    if number_of_calls is not None:
                        number_of_calls -= 1

                    delay = self.methods.events_delay(shared_params, event['service_id'])
                    weight = self.methods.interaction_weight(shared_params, event['service_id'])

                    if delay is not None:
                        events_to_handle.append({
                            'time_handle': event['time_handle'] + delay,
                            'event_type': 'incoming_event',
                            'service_id': event['service_id'],
                            'interaction_id': str(uuid.uuid4()),
                            'weight': weight
                        })

                # Алгоритм подбора оператора

                agent_id = self.methods.choose_agent(
                    shared_params,
                    interaction=Interaction(id=event['interaction_id'],
                                            service_id=event['service_id'],
                                            time_income=event['time_handle'],
                                            weight=event['weight']),
                    agents=self.state.agents)

                # Оператора удалось подобрать

                if agent_id is not None:

                    progress_time_delta = self.methods.progress_time(
                        shared_params=shared_params,
                        agent_id=agent_id,
                        service_id=event['service_id'])

                    self.state.agents[agent_id].interaction_data = Interaction(
                        id=event['interaction_id'],
                        service_id=event['service_id'],
                        time_income=event['time_handle'],
                        time_handle=event['time_handle'],
                        time_finish=event['time_handle'] + progress_time_delta,
                        weight=event['weight']
                    )
                    self.state.agents[agent_id].free_from_time = None
                    self.state.agents[agent_id].busy_from_time = event['time_handle']

                    events_to_handle.append({
                        'time_handle': event['time_handle'] + progress_time_delta,
                        'event_type': 'finish_dialog',
                        'interaction_id': event['interaction_id'],
                        'agent': agent_id,
                        'service_id': event['service_id'],
                        'handling_time': progress_time_delta
                    })

                    yield ToAgentEvent(
                        time_handle=event['time_handle'],
                        interaction_id=event['interaction_id'],
                        agent_id=agent_id,
                        service_id=event['service_id'],
                        weight=event['weight'],
                        generated_handling_time=progress_time_delta
                    )

                # Нет свободных операторов

                else:

                    waiting_time_delta = self.methods.waiting_time(
                        shared_params=shared_params,
                        service_id=event['service_id'])

                    self.state.queue.append(Interaction(
                        id=event['interaction_id'],
                        service_id=event['service_id'],
                        time_income=event['time_handle'],
                        time_churn=event['time_handle'] + waiting_time_delta,
                        weight=event['weight'])
                    )

                    yield ToQueueEvent(
                        time_handle=event['time_handle'],
                        interaction_id=event['interaction_id'],
                        service_id=event['service_id'],
                        generated_waiting_time=waiting_time_delta,
                        weight=event['weight']
                    )

                    events_to_handle.append({
                        'time_handle': event['time_handle'] + waiting_time_delta,
                        'event_type': 'leave_queue',
                        'interaction_id': event['interaction_id'],
                        'service_id': event['service_id'],
                        'waiting_time': waiting_time_delta
                    })

                changes = self.methods.services_statuses_recount_on_call(
                    shared_params=shared_params,
                    queue=self.state.queue,
                    ts=event['time_handle'])

                if changes is not None:
                    for service_id, service_changes in changes.items():
                        yield ServiceStatusChangeEvent(
                            time_handle=event['time_handle'],
                            service_id=service_id,
                            state_before=service_changes.status_before,
                            state_after=service_changes.status_after
                        )

                events_to_handle = sorted(events_to_handle,
                                               key=lambda x: x['time_handle'])

                continue

            # Событие = клиент покинул очередь до распределения

            if event['event_type'] == 'leave_queue':

                indecies = [i for i, x in enumerate(self.state.queue)
                            if x.id == event['interaction_id']]

                if len(indecies) > 0:

                    _ = self.state.queue.pop(indecies[0])
                    self.state.total_churn += 1

                    # Записать в таблицу уход клиента из очереди

                    yield ChurnEvent(
                        time_handle=event['time_handle'],
                        interaction_id=event['interaction_id'],
                        service_id=event['service_id'],
                        waiting_time=event['waiting_time']
                    )

                continue

            # Событие = оператор завершил обрабтку звонка клиента

            if event['event_type'] == 'finish_dialog':

                # Записать в табличку завершение диалога оператора и клиента

                agent_id = event['agent']

                finished_interaction = self.state.agents[agent_id].interaction_data

                self.state.agents[agent_id].interaction_data = None
                self.state.agents[agent_id].free_from_time = event['time_handle']
                self.state.agents[agent_id].busy_from_time = None
                self.state.total_finish += 1

                new_interaction = None

                yield FinishEvent(
                    time_handle=event['time_handle'],
                    interaction_id=finished_interaction.id,
                    service_id=finished_interaction.service_id,
                    agent_id=agent_id,
                    handling_time=event['handling_time']
                )

                if len(self.state.queue) > 0:

                    new_interaction = self.methods.choose_interaction(
                        shared_params=shared_params,
                        agent_id=agent_id,
                        queue=self.state.queue,
                        current_time=event['time_handle'])

                if isinstance(new_interaction, Interaction):

                    progress_time_delta = self.methods.progress_time(
                        shared_params=shared_params,
                        agent_id=agent_id,
                        service_id=new_interaction.service_id)

                    new_interaction.time_handle = event['time_handle']
                    new_interaction.time_finish = event['time_handle'] + progress_time_delta

                    self.state.agents[agent_id].interaction_data = new_interaction
                    self.state.agents[agent_id].free_from_time = None
                    self.state.agents[agent_id].busy_from_time = event['time_handle']

                    # События хоть и произойдут в одно время, но должны быть
                    # раздельными, так как у них должен быть разный interaction_id

                    yield FromQueueToAgentEvent(
                        time_handle=event['time_handle'],
                        interaction_id=new_interaction.id,
                        service_id=new_interaction.service_id,
                        weight=new_interaction.weight,
                        agent_id=agent_id,
                        waiting_time=event['time_handle'] - new_interaction.time_income,
                        generated_handling_time=progress_time_delta
                    )

                    events_to_handle.append({
                        'time_handle':  event['time_handle'] + progress_time_delta,
                        'interaction_id': new_interaction.id,
                        'service_id': new_interaction.service_id,
                        'event_type': 'finish_dialog',
                        'agent': event['agent'],
                        'handling_time': progress_time_delta
                    })

                    events_to_handle = sorted(events_to_handle,
                                                   key=lambda x: x['time_handle'])

                continue
