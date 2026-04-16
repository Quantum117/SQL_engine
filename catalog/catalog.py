from typing import Dict, List, Optional
import os

class TableSchema:
    def __init__(self, name: str, columns: Dict[str, str], file_path: Optional[str] = None):
        self.name = name
        self.columns = columns # name -> type
        self.file_path = file_path
        self.indexes: Dict[str, str] = {} # column_name -> index_type (e.g. 'HASH')

    @staticmethod
    def from_lists(name: str, col_names: List[str], col_types: List[str], file_path: Optional[str] = None):
        return TableSchema(name, dict(zip(col_names, col_types)), file_path)

    def add_index(self, column_name: str, index_type: str = 'HASH'):
        if column_name not in self.columns:
            raise ValueError(f"Column {column_name} not found in table {self.name}")
        self.indexes[column_name] = index_type

class Catalog:
    def __init__(self):
        self.tables: Dict[str, TableSchema] = {}

    def register_table(self, schema: TableSchema):
        self.tables[schema.name] = schema

    def get_table(self, name: str) -> TableSchema:
        if name not in self.tables:
            raise ValueError(f"Table {name} not found in catalog")
        return self.tables[name]
