from dataclasses import dataclass
from typing import Any, Dict, List, Type
from dataclasses_json import dataclass_json
from db_api import DBTable, DBField, DataBase, SelectionCriteria, DB_ROOT
from db_api import DBTable, DBField, DataBase, SelectionCriteria, DB_ROOT

import os
import json
import shutil

# להריץ טסט מסוים:
# py.test -k test_name
# להריץ עם מדידת זמן:
# pytest --durations=8
# להריץ את כל הטסטים:
# py.test


@dataclass_json
@dataclass
class DBField(DBField):
    name: str
    type: Type

    def __init__(self, name, type):
        self.name = name
        self.type = type


@dataclass_json
@dataclass
class SelectionCriteria(SelectionCriteria):
    field_name: str
    operator: str
    value: Any

    def __init__(self, field_name, operator, value):
        self.field_name = field_name
        self.operator = operator
        self.value = value


@dataclass_json
@dataclass
class DBTable(DBTable):
    name: str
    fields: List[DBField]
    key_field_name: str

    def __init__(self, name, fields, key_field_name):
        if key_field_name not in [filed.name for filed in fields]:
            raise KeyError
        self.key_field_name = key_field_name
        self.fields = fields
        self.name = name
        if not os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{1}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{1}.json", "w", encoding='utf-8') as data_file:
                json.dump({}, data_file)

    # TODO: ijson
    def count(self) -> int:
        count = 0
        num = 1
        while os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{num}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as file:
                count += len(json.load(file))
            num += 1

        return count

    def insert_record(self, values: Dict[str, Any]) -> None:
        # check the correctness of the keys
        if self.key_field_name not in values.keys():
            raise KeyError("the key value didn't given")

        for key in values.keys():
            if key not in [filed.name for filed in self.fields]:
                raise KeyError(f"the key {key} is not exists in the key fields")

        # add the row
        insert_file = 1
        insert_data = {}
        num = 1
        first = True
        while os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{num}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as file:
                my_data = json.load(file)

            for key in my_data.keys():
                if key == str(values[self.key_field_name]):
                    raise ValueError
            # TODO: change to 1000
            if len(my_data) < 3 and first:
                first = False
                insert_file = num
                insert_data = my_data

            num += 1

        if first:
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", "w", encoding='utf-8') as file:
                json.dump({}, file)
            insert_file = num
            insert_data = {}

        insert = values.pop(self.key_field_name)

        insert_data.update({insert: values})

        with open(f"{DB_ROOT}/{self.name}/{self.name}{insert_file}.json", "w", encoding='utf-8') as file:
            json.dump(insert_data, file)

    def delete_record(self, key: Any) -> None:
        num = 1
        while os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{num}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as f:
                my_data = json.load(f)

            if str(key) in my_data.keys():
                my_data.pop(str(key))
                with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", "w", encoding='utf-8') as file:
                    json.dump(my_data, file, ensure_ascii=False)
                return
            num += 1

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        raise NotImplementedError

    # TODO: ijson
    def get_record(self, key: Any) -> Dict[str, Any]:
        result = {"id": key}
        num = 1
        while os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{num}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as f:
                my_data = json.load(f)

            if str(key) in my_data.keys():
                result.update(my_data[str(key)])
                return result

            num += 1

        raise KeyError

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        for key in values.keys():
            if key not in [filed.name for filed in self.fields]:
                raise KeyError(f"the key {key} is not exists in the key fields")

        num = 1
        while os.path.isfile(f"{DB_ROOT}/{self.name}/{self.name}{num}.json"):
            with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", encoding='utf-8') as f:
                my_data = json.load(f)

            if str(key) in my_data.keys():
                for item in values.keys():
                    my_data[str(key)][item] = values[item]

                with open(f"{DB_ROOT}/{self.name}/{self.name}{num}.json", "w", encoding='utf-8') as file:
                    json.dump(my_data, file, ensure_ascii=False)
                return

            num += 1

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        raise NotImplementedError

    def create_index(self, field_to_index: str) -> None:
        raise NotImplementedError


@dataclass_json
@dataclass
class DataBase(DataBase):
    # Put here any instance information needed to support the API
    my_tables: Dict[str, DBTable]

    def __init__(self):
        self.my_tables = {}

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        if not os.path.isdir(f"{DB_ROOT}/{table_name}"):
            os.makedirs(f"{DB_ROOT}/{table_name}")
            new_table = DBTable(table_name, fields, key_field_name)

            with open(f"{DB_ROOT}/{table_name}/{table_name}.json", "w") as tables:
                json.dump({"name": table_name,
                           # "filed": [{field.name: field.type} for field in fields],
                           "key_field_name": key_field_name}, tables)
            self.my_tables.update({table_name: new_table})
            return new_table

        raise FileExistsError

    def num_tables(self) -> int:
        return len([name for name in os.listdir(DB_ROOT) if os.path.isdir(os.path.join(DB_ROOT, name))])

    def get_table(self, table_name: str) -> DBTable:
        if table_name in self.my_tables.keys():
            return self.my_tables[table_name]
        if os.path.isdir(f"{DB_ROOT}/{table_name}"):
            with open(f"{DB_ROOT}/{table_name}/{table_name}.json") as tables:
                table_data = json.load(tables)
                new_table = DBTable(table_data["name"],
                                    table_data["filed"],
                                    table_data["key_field_name"])
                self.my_tables[table_name] = new_table
                return new_table
        raise NameError

    def delete_table(self, table_name: str) -> None:
        if os.path.isdir(f"{DB_ROOT}/{table_name}"):
            shutil.rmtree(f"{DB_ROOT}/{table_name}", ignore_errors=True)
            if table_name in self.my_tables.keys():
                self.my_tables.pop(table_name)

    def get_tables_names(self) -> List[Any]:
        return os.listdir(DB_ROOT)

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError


my_data_base = DataBase()

student = my_data_base.create_table("student", [DBField("id", int), DBField("name", str), DBField("age", int)], "id")

student.insert_record({"id": 1, "name": "sss", "age": 78})

student.insert_record({"id": 6, "name": "kjh"})

student.insert_record({"id": 2, "age": 4})

student.insert_record({"id": 10, "name": "d"})

student.delete_record(1)

student.insert_record({"id": 18, "name": "d"})

student.update_record(18, {"name": "fd", "age": 43})

print(student.get_record(6))

# print(student.get_record(4))

nechami = my_data_base.create_table("Nechami", [DBField("learn", str), DBField("google", int), DBField("bitachon", float)], "learn")

nechami.insert_record({"learn": "bootcamp", "google": 8})

sari = my_data_base.create_table("Sari", [DBField("learn", str), DBField("google", int), DBField("bitachon", float)], "learn")

sari.insert_record({"learn": "bootcamp", "google": 8})

print(my_data_base.num_tables())

print(my_data_base.get_tables_names())

my_data_base.delete_table("Sari")

print(my_data_base.num_tables())

print(my_data_base.get_tables_names())

n = my_data_base.get_table("Nechami")

n.insert_record({"learn": "OS", "bitachon": 5.5})

student.update_record(18, {"name": "fd", "age": 43})
