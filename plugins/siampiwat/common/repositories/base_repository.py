from abc import ABC, abstractmethod
import pandas as pd

class BaseRepository(ABC):
    @abstractmethod
    def read(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, data: pd.DataFrame):
        pass
