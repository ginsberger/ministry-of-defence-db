from db_api import DBField, SelectionCriteria, DBTable, DataBase
from typing import Any, Dict, List, Type
import os


class MyDBField(DBField):
    def __init__(self, name, type):
        self.name = name
        self.type = type


class MySelectionCriteria(SelectionCriteria):
    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


class MyDBTable(DBTable):
    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name

    def count(self) -> int:
        raise NotImplementedError

    def insert_record(self, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def delete_record(self, key: Any) -> None:
        raise NotImplementedError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        raise NotImplementedError

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


class MyDataBase(DataBase):
    __data_base_id = 1

    def __init__(self, name):
        self.data_base_name = name
        self.data_base_path = "DataBases/MyDatabase" + str(MyDataBase.__data_base_id)
        self.num_table = 0
        os.mkdir(self.data_base_path)
        MyDataBase.__data_base_id += 1

    # Put here any instance information needed to support the API
    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        raise NotImplementedError

    def num_tables(self) -> int:
        raise NotImplementedError

    def get_table(self, table_name: str) -> DBTable:
        raise NotImplementedError

    def delete_table(self, table_name: str) -> None:
        raise NotImplementedError

    def get_tables_names(self) -> List[Any]:
        raise NotImplementedError

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError


if __name__ == '__main__':
    MyDataBase(DataBase)
    MyDataBase(DataBase)