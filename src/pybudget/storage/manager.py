from abc import ABC, abstractmethod
from typing import Dict

import pandas as pd


class StorageManager(ABC):

    @abstractmethod
    def get_transactions(self) -> pd.DataFrame:
        raise NotImplementedError
    
    @abstractmethod
    def update_transactions(self, updated_transactions: pd.DataFrame) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_transactions(self, start_date: str, end_date: str = None) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def load_new_transactions(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_budget(self) -> Dict[str, int]:
        raise NotImplementedError

    @abstractmethod
    def update_budget(self, new_budget: Dict[str, int]) -> None:
        raise NotImplementedError

    @abstractmethod
    def save(self) -> None:
        raise NotImplementedError
    