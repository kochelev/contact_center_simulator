from setuptools import setup, find_packages

setup(
    name="contact_center_simulator",
    version="1.0.0",
    url="https://github.com/kochelev/contact_center_simulator.git",
    author="Кочелев Николай Александрович",
    author_email="kochelev@yandex.ru",
    description="Библиотека для симуляции распределения входящего потока заявок в контактном центре",
    packages=find_packages(),    
    install_requires=[
        "deepdiff>=8.6.1",
        "fastapi>=0.118.0",
        "loguru>=0.7.3",
        "nest-asyncio>=1.6.0",
        "numpy>=2.3.3",
        "prometheus_client>=0.23.1",
        "pydantic>=2.11.10",
        "pydantic_core>=2.33.2",
        "requests>=2.32.5",
    ],
)
