# pylint: disable=missing-module-docstring
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring


from prometheus_client import Counter, Gauge, CollectorRegistry


registry = CollectorRegistry()

SCPL_ISSUES = Counter(
    'scpl_issues',
    'Общее число заявок',
    ['sid', 'kind'],
    registry=registry
)
SCPL_QUEUING = Gauge(
    'scpl_queuing',
    'Текущее число заявок',
    ['sid'],
    registry=registry
)
SCPL_INCOME = Gauge(
    'scpl_income',
    'Время между поступающими заявками',
    ['sid', 'kind'],
    registry=registry
)
SCPL_CHURN = Gauge(
    'scpl_churn',
    'Время нахождения заявки в очереди до оттока (без распределения)',
    ['sid', 'kind'],
    registry=registry
)
SCPL_HANDLING_TIME = Gauge(
    'scpl_handling_time',
    'Время обработки взаимодействия оператором',
    ['aid', 'sid', 'kind'],
    registry=registry
)

# Результаты измерения EWT и ошибка

SCPL_WAITING_TIME = Gauge(
    'scpl_waiting_time',
    'Время нахождения заявки в очереди до распределения (без оттока)',
    ['sid', 'kind', 'weight'],
    registry=registry
)
SCPL_WAITING_TIME_ERROR = Gauge(
    'scpl_waiting_time_error',
    'Ошибка предсказания времени нахождения заявки в очереди',
    ['sid', 'kind', 'weight'],
    registry=registry
)
