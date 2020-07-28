from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type


import db_api


class DBField(db_api.DBField):
    def __init__(self, name: str, type_: Type):
        self.name = name
        self.type = type_


class SelectionCriteria(db_api.SelectionCriteria):
    def __init__(self, field_name: str, operator: str, value: Any):
        self.field_name = field_name
        self.operator = operator
        self.value = value

