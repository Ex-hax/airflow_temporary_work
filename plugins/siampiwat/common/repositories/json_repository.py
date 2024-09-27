import json
from typing import Any, Dict, Union
import pandas as pd
from siampiwat.common.repositories.base_repository import BaseRepository

class JsonParser:
    def __init__(self, separator: str = "__") -> None:
        self.separator = separator

    def flatten(self, data: Union[Dict[str, Any], list]) -> Dict[str, Any]:
        flattened_data: Dict[str, Any] = {}
        self._flatten_recursive(data, flattened_data)
        return flattened_data

    def _flatten_recursive(self, data: Union[Dict[str, Any], list, Any], flattened_data: Dict[str, Any], parent_key: str = "") -> None:
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}{self.separator}{key}" if parent_key else key
                if isinstance(value, (dict, list)):
                    self._flatten_recursive(value, flattened_data, new_key)
                else:
                    flattened_data[new_key] = value
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{parent_key}{self.separator}{i}"
                self._flatten_recursive(item, flattened_data, new_key)
        else:
            flattened_data[parent_key] = data

    def parse_json_to_dataframe(self, json_data: Union[str, Dict[str, Any], list]) -> pd.DataFrame:
        if isinstance(json_data, str):
            json_data = json.loads(json_data)
        flattened_data = self.flatten(json_data)
        return pd.DataFrame([flattened_data])

class JsonRepository(BaseRepository):
    def __init__(self, json_data: Union[str, Dict[str, Any], list]) -> None:
        self.parser = JsonParser()
        self.json_data = json_data

    def read(self) -> pd.DataFrame:
        return self.parser.parse_json_to_dataframe(self.json_data)

    def write(self, data: pd.DataFrame) -> None:
        pass
