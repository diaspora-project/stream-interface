from __future__ import annotations
from abc import ABC, abstractmethod


class BaseStream(ABC):
    """

    Args:
        ABC (_type_): _description_
    """

    @abstractmethod
    def producer(self):
        pass

    @abstractmethod
    def consumer(self):
        pass


class Producer(ABC):
    """_summary_

    Args:
        ABC (_type_): _description_
    """

    @abstractmethod
    def send():
        pass

    @abstractmethod
    def flush():
        pass


class Consumer(ABC):
    """

    Args:
        ABC (_type_): _description_
    """

    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __next__(self):
        pass

    @abstractmethod
    def close():
        pass
