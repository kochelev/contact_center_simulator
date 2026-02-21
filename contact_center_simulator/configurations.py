# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=invalid-name


import math
from random import choices, randrange, sample, random
from typing import List, Dict, Optional
from abc import ABC, abstractmethod
import numpy as np
from pydantic import BaseModel

from .types import (
    ServiceStatusChange,
    Interaction,
    Agent
)


class Configuration(ABC):

    "Абстрактный класс, интерфейс для любой конфигурации симулятора."

    # КОНСТАНТЫ ПЛАТФОРМЫ (ДЛЯ ВСЕХ КОНФИГУРАЦИЙ)

    services_weights = [.1, .2, .3, .4, .5, .6, .7, .8, .9, 1., 2., 3., 4., 5., 6., 7., 8., 9., 10.]


    def __init__(self):
        pass


    # ГЕНЕРАЦИЯ ПАРАМЕТРОВ СРЕДЫ

    @abstractmethod
    def gen_params(
        self) -> dict:

        '''
        Функция генерации параметров среды.
        '''

    # ГЕНЕРАЦИЯ ЧИСЕЛ


    @abstractmethod
    def events_delay(
        self,
        shared_params,
        service_id: str) -> Optional[float]:

        '''
        Функция генерации задержки между двумя входящими звонками
        определенного сервиса.
        Если у сервиса нулевая интенсивность, возвращается None.
        '''


    @abstractmethod
    def interaction_weight(
        self,
        shared_params,
        service_id: str) -> float:

        '''
        Функция генерации веса взаимодействия в определенном сервисе.
        '''


    @abstractmethod
    def waiting_time(
        self,
        shared_params,
        service_id: str) -> float:

        '''
        Функция генерации времени нахождения взаимодействия
        в очереди, после истечения которого клиент кладет
        трубку, недождавшись ответа.
        '''


    @abstractmethod
    def progress_time(
        self,
        shared_params,
        agent_id: str,
        service_id: str) -> float:

        '''
        Функция генерации времени обработки оператором
        взаимодействия по определенному сервису.
        '''


    # ПОДБОРА ОПЕРАТОРА И ВЗАИМОДЕЙСТВИЯ


    @abstractmethod
    def choose_agent(
        self,
        shared_params,
        interaction: Interaction,
        agents: Dict[str, Agent]) -> Optional[str]:

        '''
        Функция подбора оператора, возвращается его идентификатор.
        Если нет свободных, то возвращается None.
        '''


    @abstractmethod
    def choose_interaction(
        self,
        shared_params,
        agent_id: str,
        queue: List[Interaction],
        current_time: float) -> Optional[Interaction]:

        '''
        Функция подбора взаимодействия, возвращается его идентификатор.
        Если нет подходящих в очереди, то возвращается None.
        '''


    # УПРАВЛЕНИЕ РЕЗЕРВАМИ СЕРВИСОВ


    def services_statuses_recount_on_timer(
        self,
        shared_params,
        queue: List[Interaction],
        ts: int) -> Optional[Dict[str, ServiceStatusChange]]:

        '''
        Функция пересчета статусов сервисов (резервов) по таймеру.
        '''


    def services_statuses_recount_on_call(
        self,
        shared_params,
        queue: List[Interaction],
        ts: int) -> Optional[Dict[str, ServiceStatusChange]]:

        '''
        Функция пересчета статусов сервисов (резервов) при каждом новом входящем звонке.
        '''


    @staticmethod
    def generate_matrix_numpy(n, m, x):

        '''
        Функция генерирует матрицу распределения сервисов по операторам
        на основе количества операторов, сервисов и максимально допустимому
        числу сервисов на одном операторе.
        '''

        assert n >= m, \
            'Число операторов должно быть больше числа сервисов'
        assert x > 0 and x <= m, \
            'Максимальное число единиц должно быть больше 0 и не больше числа сервисов'

        matrix = np.zeros((n, m))

        # Каждый оператор должен обрабатывать только один сервис
        for i in range(n):
            j = randrange(0, m)
            matrix[i, j] = 1

        # Каждый сервис должен обрабатываться хотя бы одним оператором
        for j in range(m):
            if sum(matrix[:, j]) == 0.0:
                i = randrange(0, n)
                matrix[i, :] = 0
                matrix[i, j] = 1

        # Заполняем сервисы у операторов согласно лимитам на число сервисов

        for i in range(n):

            # Генерируем число обрабатываемых данным оператором сервисов
            k = randrange(1, x+1)

            # Если оператор должен обрабатывать один сервис, то он уже есть
            if k == 1:
                continue

            # Отбираем индексы в строке, где значение 0
            indices = np.where(matrix[i, :] == 0)[0].astype(np.int16)

            # Случайно выбираем индексы сервисов, которые должен обрабатывать оператор
            to_change = sample(list(indices), k-1)

            # Проставляем отобранным сервисам единицы в матрице
            for j in to_change:
                matrix[i, j] = 1

        return matrix.astype(int).tolist()


# ИМПЛЕМЕНТАЦИИ КОНФИГУРАЦИЙ


class ConfigurationEEE(Configuration):

    '''
    Время между поступающими звонками одного сервиса - экспоненциальное распределение.
    Время нахождения клиента в очереди без распределения в рамках одного сервиса - экспоненциальное распределение.
    Время обработки заявки определенным оператором по одному сервису - экспоненциальное распределение.
    '''

    def gen_params(
        self,
        agents_range: tuple[int, int],
        services_range: tuple[int, int],
        services_intensities_range: tuple[float, float],
        services_churn_range: tuple[float, float],
        max_s_per_a_number: int,
        agents_handling_time_mean_range: tuple[float, float]):

        assert agents_range[0] <= agents_range[1], \
            'Минимальное число операторов должно быть меньше или равно максимальному'
        assert services_range[0] <= services_range[1], \
            'Минимальное число сервисов должно быть меньше или равно максимальному'
        assert agents_range[0] >= services_range[0], \
            'Минимальное число операторов должно быть больше или равно минимальному числу сервисов'

        # Число операторов
        a = randrange(agents_range[0], agents_range[1]+1)

        # Число сервисов (не более числа операторов)
        s = randrange(services_range[0], min(a, services_range[0])+1)

        a_len = len(str(a))
        s_len = len(str(s))

        # Максимальное число сервисов на операторе (не более числа сервисов)
        max_s_per_a_number = min(max_s_per_a_number, s)

        # Идентификаторы операторов
        agent_ids = [f'A_{str(i).zfill(a_len)}' for i in range(1, a+1)]

        # Идентификаторы сервисов
        service_ids = [f'S_{str(j).zfill(s_len)}' for j in range(1, s+1)]

        # Подключение сервисов операторам
        agent_per_service_activity_matrix = self.generate_matrix_numpy(a, s, max_s_per_a_number)

        agent_per_service_activity = {
            agent_ids[i]: {
                service_ids[j]: v for j, v in enumerate(agent_services)
            } for i, agent_services in enumerate(agent_per_service_activity_matrix)}

        # Средние значения интервалов между последовательными звонками
        services_intensities = {sid: random() * (services_intensities_range[1] - services_intensities_range[0]) + services_intensities_range[0]
                                for sid in service_ids}

        # Вероятности весов по сервисам
        services_weights_probabilities = {
            sid: [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0] for sid in service_ids
        }

        # Средние значения времени нахождения в очереди без распределения
        services_churn = {sid: randrange(services_churn_range[0],
                                         services_churn_range[1]+1)
                                for sid in service_ids}

        # Среднее и СКО времени обработки звонков операторами в разных сервисах
        agents_handling_time = {
            aid: {
                sid: {
                    'mean': randrange(agents_handling_time_mean_range[0],
                                      agents_handling_time_mean_range[1]+1)
                }
                for sid in service_ids if agent_per_service_activity[aid][sid] == 1
            } for aid in agent_ids
        }

        return {
            'agent_ids': agent_ids,
            'service_ids': service_ids,
            'services_intensities': services_intensities,
            'services_weights_probabilities': services_weights_probabilities,
            'services_churn': services_churn,
            'agent_per_service_activity': agent_per_service_activity,
            'agents_handling_time': agents_handling_time
        }


    def events_delay(
        self,
        shared_params,
        service_id: str) -> Optional[float]:

        assert shared_params['services_intensities'][service_id] >= 0, \
            'Количество событий, поступающих за время, не может быть отрицательным'

        if shared_params['services_intensities'][service_id] == 0:
            return None

        # Показательное распределение временных промежутков
        # между ближайшими событиями.

        return float(np.random.exponential(
            scale=shared_params['services_intensities'][service_id],size=1)[0])


    def interaction_weight(
        self,
        shared_params,
        service_id: str) -> float:

        return choices(
            population=self.services_weights,
            weights=list(shared_params['services_weights_probabilities'][service_id]),
            k=1)[0]


    def waiting_time(
        self,
        shared_params,
        service_id: str) -> float:

        # Показательное распределение временных промежутков
        # между ближайшими событиями.

        return float(np.random.exponential(
            scale=shared_params['services_churn'][service_id],
            size=1)[0])


    def progress_time(
        self,
        shared_params,
        agent_id: str,
        service_id: str) -> float:

        # Логнормальное распределение временных промежутков
        # между ближайшими событиями.

        mean = shared_params['agents_handling_time'][agent_id][service_id]['mean']

        return float(np.random.exponential(
            scale=mean,size=1)[0])


    def choose_agent(
        self,
        shared_params,
        interaction: Interaction,
        agents: Dict[str, Agent]) -> Optional[str]:

        # Выборка свободных операторов (самый долгождущий)

        free_agent_ids = [agent_id for agent_id, agent in agents.items()
                          if agent.interaction_data is None]

        if len(free_agent_ids) == 0:
            return None

        # Из списка свободных операторов отбираем тех, у которых
        # необходимый сервис подключен и активен

        agent_ids = [k for k, v in shared_params['agent_per_service_activity'].items()
                     if k in free_agent_ids and interaction.service_id in v and \
                        v[interaction.service_id]]

        # Ничего не возвращаем, если нет ни одного свободного оператора в нужном сервисе

        if len(agent_ids) == 0:
            return None

        # С помощью отобранных ID операторов формируем справочник Оператор -> Время освобождения

        agent_per_free_from_time = {agent_id: agents[agent_id].free_from_time
                                    for agent_id in agent_ids}

        # print('HHH', agent_per_free_from_time)

        # Возвращаем ID оператора, у которого время освобождения
        # самое маленькое (раньше всех освободился и дольше всех в простое)

        choosen_agent_id = min(agent_per_free_from_time, key=agent_per_free_from_time.get)

        # print('YYY', choosen_agent_id)

        return choosen_agent_id


    def choose_interaction(
        self,
        shared_params,
        agent_id: str,
        queue: List[Interaction],
        current_time: float) -> Optional[Interaction]:

        # Сначала определяем список сервисов, которые обслуживает оператор
        services = [k for k, v
                    in shared_params['agent_per_service_activity'][agent_id].items() if v]

        # Затем из очереди подбираем взаимодействия с соответствующими сервисами

        interactions = [(i, (current_time - x.time_income) * x.weight,
                         current_time - x.time_income, x.weight)
                        for i, x in enumerate(queue) if x.service_id in services]

        # Рассчитываем время ожидания в очереди согласно весу взаимодействий

        interactions = sorted(interactions, key=lambda x: x[1], reverse=True)

        if len(interactions) > 0:
            new_i = interactions[0][0] # В начале очереди всегда наиболее давние звонки

            return queue.pop(new_i)

        return None


class ParamsEEE(BaseModel):
    services_intensities: Optional[Dict[str, float]] = None
    services_weights_probabilities: Optional[Dict[str, List[float]]] = None
    agents_handling_time: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None
    services_churn: Optional[Dict[str, float]] = None


class ConfigurationEEL(Configuration):


    def gen_params(
        self,
        agents_range: tuple[int, int],
        services_range: tuple[int, int],
        services_intensities_range: tuple[float, float],
        services_churn_range: tuple[float, float],
        max_s_per_a_number: int,
        agents_handling_time_mean_range: tuple[float, float],
        agents_handling_time_std_range: tuple[float, float]):

        assert agents_range[0] <= agents_range[1], \
            'Минимальное число операторов должно быть меньше или равно максимальному'
        assert services_range[0] <= services_range[1], \
            'Минимальное число сервисов должно быть меньше или равно максимальному'
        assert agents_range[0] >= services_range[0], \
            'Минимальное число операторов должно быть больше или равно минимальному числу сервисов'

        # Число операторов
        a = randrange(agents_range[0], agents_range[1]+1)

        # Число сервисов (не более числа операторов)
        s = randrange(services_range[0], min(a, services_range[0])+1)

        a_len = len(str(a))
        s_len = len(str(s))

        # Максимальное число сервисов на операторе (не более числа сервисов)
        max_s_per_a_number = min(max_s_per_a_number, s)

        # Идентификаторы операторов
        agent_ids = [f'A_{str(i).zfill(a_len)}' for i in range(1, a+1)]

        # Идентификаторы сервисов
        service_ids = [f'S_{str(j).zfill(s_len)}' for j in range(1, s+1)]

        # Подключение сервисов операторам
        agent_per_service_activity_matrix = self.generate_matrix_numpy(a, s, max_s_per_a_number)

        agent_per_service_activity = {
            agent_ids[i]: {
                service_ids[j]: v for j, v in enumerate(agent_services)
            } for i, agent_services in enumerate(agent_per_service_activity_matrix)}

        # Средние значения интервалов между последовательными звонками
        services_intensities = {sid: random() * (services_intensities_range[1] - services_intensities_range[0]) + services_intensities_range[0]
                                for sid in service_ids}

        # Вероятности весов по сервисам
        services_weights_probabilities = {
            sid: [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0] for sid in service_ids
        }

        # Средние значения времени нахождения в очереди без распределения
        services_churn = {sid: randrange(services_churn_range[0],
                                         services_churn_range[1]+1)
                                for sid in service_ids}

        # Среднее и СКО времени обработки звонков операторами в разных сервисах
        agents_handling_time = {
            aid: {
                sid: {
                    'mean': randrange(agents_handling_time_mean_range[0],
                                      agents_handling_time_mean_range[1]+1),
                    'std': randrange(agents_handling_time_std_range[0],
                                     agents_handling_time_std_range[1]+1)
                }
                for sid in service_ids if agent_per_service_activity[aid][sid] == 1
            } for aid in agent_ids
        }

        return {
            'agent_ids': agent_ids,
            'service_ids': service_ids,
            'services_intensities': services_intensities,
            'services_weights_probabilities': services_weights_probabilities,
            'services_churn': services_churn,
            'agent_per_service_activity': agent_per_service_activity,
            'agents_handling_time': agents_handling_time
        }


    def events_delay(
        self,
        shared_params,
        service_id: str) -> Optional[float]:

        assert shared_params['services_intensities'][service_id] >= 0, \
            'Количество событий, поступающих за время, не может быть отрицательным'

        if shared_params['services_intensities'][service_id] == 0:
            return None

        # Показательное распределение временных промежутков
        # между ближайшими событиями.

        return float(np.random.exponential(
            scale=shared_params['services_intensities'][service_id],size=1)[0])


    def interaction_weight(
        self,
        shared_params,
        service_id: str) -> float:

        return choices(
            population=self.services_weights,
            weights=list(shared_params['services_weights_probabilities'][service_id]),
            k=1)[0]


    def waiting_time(
        self,
        shared_params,
        service_id: str) -> float:

        # Показательное распределение временных промежутков
        # между ближайшими событиями.

        return float(np.random.exponential(
            scale=shared_params['services_churn'][service_id],
            size=1)[0])


    def progress_time(
        self,
        shared_params,
        agent_id: str,
        service_id: str) -> float:

        # Логнормальное распределение временных промежутков
        # между ближайшими событиями.

        mean = shared_params['agents_handling_time'][agent_id][service_id]['mean']
        std = shared_params['agents_handling_time'][agent_id][service_id]['std']

        my_time = float(np.random.lognormal(
            mean=math.log(mean ** 2 / math.sqrt(std ** 2 + mean ** 2)),
            sigma=math.sqrt(math.log((std / mean) ** 2 + 1)),
            size=1)[0])

        return my_time


    def choose_agent(
        self,
        shared_params,
        interaction: Interaction,
        agents: Dict[str, Agent]) -> Optional[str]:

        # Выборка свободных операторов (самый долгождущий)

        free_agent_ids = [agent_id for agent_id, agent in agents.items()
                          if agent.interaction_data is None]
        
        if len(free_agent_ids) == 0:
            return None

        # Из списка свободных операторов отбираем тех, у которых
        # необходимый сервис подключен и активен

        agent_ids = [k for k, v in shared_params['agent_per_service_activity'].items()
                     if k in free_agent_ids and interaction.service_id in v and \
                        v[interaction.service_id]]

        # Ничего не возвращаем, если нет ни одного свободного оператора в нужном сервисе

        if len(agent_ids) == 0:
            return None

        # С помощью отобранных ID операторов формируем справочник Оператор -> Время освобождения

        agent_per_free_from_time = {agent_id: agents[agent_id].free_from_time
                                    for agent_id in agent_ids}

        # Возвращаем ID оператора, у которого время освобождения
        # самое маленькое (раньше всех освободился и дольше всех в простое)

        choosen_agent_id = min(agent_per_free_from_time, key=agent_per_free_from_time.get)

        return choosen_agent_id


    def choose_interaction(
        self,
        shared_params,
        agent_id: str,
        queue: List[Interaction],
        current_time: float) -> Optional[Interaction]:

        # Сначала определяем список сервисов, которые обслуживает оператор
        services = [k for k, v
                    in shared_params['agent_per_service_activity'][agent_id].items() if v]

        # Затем из очереди подбираем взаимодействия с соответствующими сервисами

        interactions = [(i, (current_time - x.time_income) * x.weight,
                         current_time - x.time_income, x.weight)
                        for i, x in enumerate(queue) if x.service_id in services]

        # Рассчитываем время ожидания в очереди согласно весу взаимодействий

        interactions = sorted(interactions, key=lambda x: x[1], reverse=True)

        if len(interactions) > 0:
            new_i = interactions[0][0] # В начале очереди всегда наиболее давние звонки

            return queue.pop(new_i)

        return None


class ParamsEEL(BaseModel):
    services_intensities: Optional[Dict[str, float]] = None
    services_weights_probabilities: Optional[Dict[str, List[float]]] = None
    agents_handling_time: Optional[Dict[str, Dict[str, Dict[str, float]]]] = None
    services_churn: Optional[Dict[str, float]] = None
