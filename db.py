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


