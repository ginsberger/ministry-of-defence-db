from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
import shutil
import os
from bson import BSON
import bson
from operator import eq, ne, gt, lt, le, ge, is_, is_not

import db_api

operator_dict = {"<": lt, ">": gt, "=": eq, "!=": ne, "<=": le, ">=": ge, "is": is_, "is not": is_not}


class DBField(db_api.DBField):
    def __init__(self, name: str, type_: Type):
        self.name = name
        self.type = type_


class SelectionCriteria(db_api.SelectionCriteria):
    def __init__(self, field_name: str, operator: str, value: Any):
        self.field_name = field_name
        self.operator = operator
        self.value = value


class DBTable(db_api.DBTable):
    __max_rows = 1000

    def __init__(self, name: str, fields: List[DBField], key_field_name: str, data_base_path=""):
        self.__NAME = name
        self.__FIELDS = fields
        self.__KEY_FIELD_NAME = key_field_name
        self.__MY_PATH = os.path.join(data_base_path, name)
        self.__PATH = data_base_path
        self.__BECKUP_PATH = os.path.join("database_exercise/db_files_backup", name)
        self.__num_rows = 0
        self.__num_of_blocks = 1
        self.__blocks_have_place = {1: 1000}
        self.__indexes = {self.__KEY_FIELD_NAME}
        with open(self.__MY_PATH + "_key_index.bson", "wb") as bson_file:
            bson_file.write(BSON.encode(dict()))

        with open(self.__MY_PATH + "1.bson", "wb") as bson_file:
            bson_file.write(BSON.encode(dict()))

    def count(self) -> int:
        return self.__num_rows

    def insert_record(self, values: Dict[str, Any]) -> None:
        # להוסיף טיפול ב-self.__blocks_have_place
        if self.__get_path_of_key(str(values[self.__KEY_FIELD_NAME])) is not None:
            raise ValueError
        else:
            try:
                for field in self.__FIELDS:
                    if type(values[field.name]) != field.type:
                        pass
            except KeyError:
                raise ValueError
            with open(self.__MY_PATH + "1.bson", "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                dict_[str(values[self.__KEY_FIELD_NAME])] = values
                print(dict_)
            with open(self.__MY_PATH + "1.bson", "wb") as bson_file:
                bson_file.write(BSON.encode(dict_))

            self.__num_rows += 1
            with open(self.__MY_PATH + "_key_index.bson", "rb") as bson_file:
                keys_dict = bson.decode_all(bson_file.read())[0]
                keys_dict[str(values[self.__KEY_FIELD_NAME])] = self.__MY_PATH + "1.bson"
            with open(self.__MY_PATH + "_key_index.bson", "wb") as bson_file:
                bson_file.write(BSON.encode(keys_dict))

    def delete_record(self, key: Any) -> None:
        path = self.__get_path_of_key(str(key))
        if path is not None:
            with open(path, "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                del dict_[str(key)]
            with open(path, "wb") as bson_file:
                bson_file.write(BSON.encode(dict_))

            with open(self.__MY_PATH + "_key_index.bson", "rb") as bson_file:
                keys_dict = bson.decode_all(bson_file.read())[0]
                del keys_dict[str(key)]
            with open(self.__MY_PATH + "_key_index.bson", "wb") as bson_file:
                bson_file.write(BSON.encode(keys_dict))
            self.__num_rows -= 1
        else:
            raise ValueError

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        # צריך לממש אופטימיזציות עם אינדקס
        keys_to_delete = []
        for i in range(self.__num_of_blocks):
            with open(self.__MY_PATH + f"{i + 1}.bson", "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                for key in dict_.keys():
                    if self.__is_meets_conditions(dict_[key], criteria):
                        keys_to_delete.append(key)
                for key in keys_to_delete:
                    del dict_[str(key)]

            with open(self.__MY_PATH + f"{i + 1}.bson", "wb") as bson_file:
                bson_file.write(BSON.encode(dict_))

        with open(self.__MY_PATH + "_key_index.bson", "rb") as bson_file:
            keys_dict = bson.decode_all(bson_file.read())[0]
            for key in keys_to_delete:
                del keys_dict[key]
                self.__num_rows -= 1
        with open(self.__MY_PATH + "_key_index.bson", "wb") as bson_file:
            bson_file.write(BSON.encode(keys_dict))

    def get_record(self, key: Any) -> Dict[str, Any]:
        path = self.__get_path_of_key(str(key))
        if path is not None:
            with open(path, "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                return dict_[str(key)]
        return {}

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        path = self.__get_path_of_key(str(key))
        if path is not None:
            with open(path, "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                dict_[str(key)].update(values)
            with open(path, "wb") as bson_file:
                bson_file.write(BSON.encode(dict_))
        else:
            print("the key is not exist")

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        # צריך לעשות אופטימיזציות
        query_list = []
        for i in range(self.__num_of_blocks):
            with open(self.__MY_PATH + f"{i + 1}.bson", "rb") as bson_file:
                dict_ = bson.decode_all(bson_file.read())[0]
                for key in dict_.keys():
                    if self.__is_meets_conditions(dict_[str(key)], criteria):
                        query_list.append(dict_[str(key)])

        return query_list

    def __get_path_of_key(self, key: Any) -> str:
        with open(self.__MY_PATH + "_key_index.bson", "rb") as bson_file:
            keys_dict = bson.decode_all(bson_file.read())
            try:
                return keys_dict[0][key]
            except KeyError:
                return None

    def __is_meets_conditions(self, item, criteria: List[SelectionCriteria]) -> bool:
        for select in criteria:
            first = item[select.field_name]
            operator = select.operator
            value = select.value
            if not operator_dict[operator](first, value):
                return False
        return True


class DataBase(db_api.DataBase):
    __PATH = Path("db_files")
    __TABLES = dict()
    __BACKUP_PATH = Path("db_files_backup")

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        if key_field_name not in [field.name for field in fields]:
            raise ValueError
        try:
            table_path = os.path.join(DataBase.__PATH, table_name)
            os.mkdir(table_path)
            new_table = DBTable(table_name, fields, key_field_name, table_path)
            DataBase.__TABLES[table_name] = new_table
            return new_table
        except FileExistsError:
            print(f"{table_name} is already exist")
            return DataBase.__TABLES[table_name]

    def num_tables(self) -> int:
        return len(DataBase.__TABLES)

    def get_table(self, table_name: str) -> DBTable:
        return DataBase.__TABLES[table_name]

    def delete_table(self, table_name: str) -> None:
        try:
            shutil.rmtree(os.path.join(DataBase.__PATH, table_name))
            del DataBase.__TABLES[table_name]
        except FileNotFoundError:
            print(f"Failed to delete table {table_name}")

    def get_tables_names(self) -> List[Any]:
        return list(DataBase.__TABLES.keys())

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        pass
