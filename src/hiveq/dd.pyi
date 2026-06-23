from typing import Any, Dict, List, Optional, Union

import pandas as pd

class DateRange:
    start: str
    end: str
    date_list: List[str]
    def __init__(self, start: str, end: str) -> None: ...
    def chunks(self, n: int) -> List[List[str]]: ...
    def __iter__(self) -> Any: ...

class TimeRange:
    start: str
    end: str
    time_list: List[str]
    def __init__(self, start: str, end: str) -> None: ...
    def __iter__(self) -> Any: ...

class Driver:
    def load(
        self,
        dataset: Optional[str] = None,
        schema: Optional[str] = None,
        symbols: Optional[Union[str, List[str]]] = None,
        date: Optional[DateRange] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        time: Optional[TimeRange] = None,
        filter_mode: Optional[str] = None,
        split_size: Optional[int] = None,
        columns: Optional[List[str]] = None,
        limit: Optional[int] = None,
        path: Optional[str] = None,
        topic: Optional[str] = None,
        keys: Optional[List[str]] = None,
        mode: Optional[str] = None,
        drop_duplicates: Optional[List[str]] = None,
        timezone: Optional[str] = None,
        time_out: Optional[float] = None,
    ) -> Optional[pd.DataFrame]: ...
    def save(
        self,
        df: pd.DataFrame,
        schema: Optional[str] = None,
        key: Optional[str] = None,
        dataset: Optional[str] = None,
        path: Optional[str] = None,
        output_columns: Optional[Dict[str, str]] = None,
        column_map: Optional[Dict[str, str]] = None,
        defaults: Optional[Dict[str, Any]] = None,
        required_fields: Optional[List[str]] = None,
        operation: str = 'add',
        async_mode: bool = False,
    ) -> Optional[pd.DataFrame]: ...
    def stop(self) -> None: ...

def load(
    dataset: Optional[str] = None,
    schema: Optional[str] = None,
    symbols: Optional[Union[str, List[str]]] = None,
    date: Optional[DateRange] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    time: Optional[TimeRange] = None,
    filter_mode: Optional[str] = None,
    split_size: Optional[int] = None,
    columns: Optional[List[str]] = None,
    limit: Optional[int] = None,
    path: Optional[str] = None,
    topic: Optional[str] = None,
    keys: Optional[List[str]] = None,
    mode: Optional[str] = None,
    drop_duplicates: Optional[List[str]] = None,
    timezone: Optional[str] = None,
    time_out: Optional[float] = None,
) -> Optional[pd.DataFrame]: ...

def save(
    df: pd.DataFrame,
    schema: Optional[str] = None,
    key: Optional[str] = None,
    dataset: Optional[str] = None,
    path: Optional[str] = None,
    output_columns: Optional[Dict[str, str]] = None,
    column_map: Optional[Dict[str, str]] = None,
    defaults: Optional[Dict[str, Any]] = None,
    required_fields: Optional[List[str]] = None,
    operation: str = 'add',
    async_mode: bool = False,
) -> Optional[pd.DataFrame]: ...

def stop() -> None: ...
