from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type
from dataclasses_json import dataclass_json

import os
import json


@dataclass_json
@dataclass
class DBField:
    name: str
    type: Type

    def __init__(self, name, type):
        self.name = name
        self.type = type


@dataclass_json
@dataclass
class DBTable:
    name: str
    fields: List[DBField]
    key_field_name: str

    def __init__(self, name, fields, key_field_name):
        if key_field_name not in [filed.name for filed in fields]:
            raise KeyError
        self.key_field_name = key_field_name
        self.fields = fields
        self.name = name
        with open(f"{'DB'}/{self.name}.json", "w", encoding='utf-8') as data_file:
            json.dump({}, data_file)

    # TODO: ijson
    def count(self) -> int:
        with open(f"{'DB'}/{self.name}.json", encoding='utf-8') as file:
            my_data = json.load(file)

        return len(my_data)

    def insert_record(self, values: Dict[str, Any]) -> None:
        # check the correctness of the keys
        if self.key_field_name not in values.keys():
            raise KeyError("the key value didn't given")

        for key in values.keys():
            if key not in [filed.name for filed in self.fields]:
                raise KeyError(f"the key {key} is not exists in the key fields")

        # add the row
        with open(f"{'DB'}/{self.name}.json", encoding='utf-8') as file:
            my_data = json.load(file)

        for key in my_data.keys():
            if key == values[self.key_field_name]:
                raise ValueError

        insert = values.pop(self.key_field_name)

        my_data.update({insert: values})

        with open(f"{'DB'}/{self.name}.json", "w", encoding='utf-8') as file:
            json.dump(my_data, file)

    def delete_record(self, key: Any) -> None:
        with open(f"{'DB'}/{self.name}.json", encoding='utf-8') as f:
            my_data = json.load(f)

        my_data.pop(str(key))

        with open(f"{'DB'}/{self.name}.json", "w", encoding='utf-8') as file:
            json.dump(my_data, file, ensure_ascii=False)

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    # TODO: ijson
    def get_record(self, key: Any) -> Dict[str, Any]:
        with open(f"{'DB'}/{self.name}.json", encoding='utf-8') as f:
            my_data = json.load(f)

        result = {"id": key}
        result.update(my_data[str(key)])
        return result

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with open(f"{'DB'}/{self.name}.json", encoding='utf-8') as f:
            my_data = json.load(f)

        for item in values.keys():
            my_data[key][item] = values[item]

        with open(f"{'DB'}/{self.name}.json", "w", encoding='utf-8') as file:
            json.dump(my_data, file, ensure_ascii=False)

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase:
    # Put here any instance information needed to support the API
    my_tables: Dict[str: DBTable]

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        self.my_tables.update({table_name: DBTable(table_name, fields, key_field_name)})
        return self.my_tables[table_name]

    def num_tables(self) -> int:
        return len(self.my_tables)

    def get_table(self, table_name: str) -> DBTable:
        return self.my_tables[table_name]

    def delete_table(self, table_name: str) -> None:
        if table_name in self.my_tables.keys():
            os.remove(f"{'DB'}/{table_name}.json")
        self.my_tables.pop(table_name)

    def get_tables_names(self) -> List[Any]:
        return self.my_tables.keys()

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
