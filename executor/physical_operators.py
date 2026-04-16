import csv
import os
import time
from typing import Dict, Any, List, Optional, Tuple, Union
from ast_nodes.nodes import ColumnRef, Literal, BinaryExpression, LogicalExpression, AggregateExpression

class PhysicalOperator:
    def __init__(self):
        self.stats = {"processed_rows": 0, "time_ms": 0.0}

    def __iter__(self):
        return self

    def __next__(self) -> Dict[str, Any]:
        raise NotImplementedError

    def get_stats(self) -> Dict[str, int]:
        return self.stats

    def evaluate(self, expr, row: Dict[str, Any], row2: Optional[Dict[str, Any]] = None) -> Any:
        """
        Improved evaluation with zero-allocation join support.
        If row2 is provided, it searches both rows for column references.
        """
        if isinstance(expr, ColumnRef):
            # Cache the qualified name on the expression object to avoid f-string overhead
            if not hasattr(expr, "_cached_qual"):
                expr._cached_qual = f"{expr.table}.{expr.name}" if expr.table else expr.name
            
            qual_name = expr._cached_qual
            if qual_name in row: return row[qual_name]
            if not expr.table and expr.name in row: return row[expr.name]
            
            if row2 is not None:
                if qual_name in row2: return row2[qual_name]
                if not expr.table and expr.name in row2: return row2[expr.name]
            
            return None
            
        if isinstance(expr, Literal):
            return expr.value
            
        if isinstance(expr, BinaryExpression):
            left = self.evaluate(expr.left, row, row2)
            right = self.evaluate(expr.right, row, row2)
            op = expr.operator
            if op == '=': return left == right
            if op == '!=': return left != right
            if op == '>': return left > right
            if op == '<': return left < right
            if op == '>=': return left >= right
            if op == '<=': return left <= right
            if op == '+': return left + right
            if op == '-': return left - right
            
        if isinstance(expr, LogicalExpression):
            left = self.evaluate(expr.left, row, row2)
            if expr.operator == 'AND':
                if not left: return False
                return self.evaluate(expr.right, row, row2)
            if expr.operator == 'OR':
                if left: return True
                return self.evaluate(expr.right, row, row2)
                
        if isinstance(expr, AggregateExpression):
            key = f"{expr.func}({expr.expression.pretty()})"
            return row.get(key)
            
        return True

class PhysicalScan(PhysicalOperator):
    def __init__(self, table_name: str, columns: List[str], file_path: str, alias: Optional[str] = None):
        super().__init__()
        self.table_name = table_name
        self.columns = columns
        self.file_path = file_path
        self.alias = alias or table_name
        self.file = None
        self.reader = None
        # Pre-cache keys for performance
        self._target_cols = [k for k in self.columns]
        self._prefixed_keys = [f"{self.alias}.{k}" for k in self.columns]

    def __next__(self) -> Dict[str, Any]:
        start = time.perf_counter()
        if self.file is None:
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(f"CSV file not found: {self.file_path}")
            self.file = open(self.file_path, 'r')
            self.reader = csv.DictReader(self.file)

        try:
            row = next(self.reader)
            # Use zip and pre-cached keys for speed
            prefixed_row = {pk: self._parse_val(row[rk]) for pk, rk in zip(self._prefixed_keys, self._target_cols)}
            self.stats["processed_rows"] += 1
            return prefixed_row
        finally:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000

    def _parse_val(self, v):
        try:
            if '.' in v: return float(v)
            return int(v)
        except ValueError:
            return v

    def __del__(self):
        if hasattr(self, 'file') and self.file:
            self.file.close()

class PhysicalFilter(PhysicalOperator):
    def __init__(self, input: PhysicalOperator, condition: Any):
        super().__init__()
        self.input = input
        self.condition = condition

    def __next__(self) -> Dict[str, Any]:
        start = time.perf_counter()
        try:
            while True:
                row = next(self.input)
                if self.evaluate(self.condition, row):
                    self.stats["processed_rows"] += 1
                    return row
        finally:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000

class PhysicalProject(PhysicalOperator):
    def __init__(self, input: PhysicalOperator, expressions: List[Any], aliases: List[Optional[str]]):
        super().__init__()
        self.input = input
        self.expressions = expressions
        self.aliases = aliases

    def __next__(self) -> Dict[str, Any]:
        start = time.perf_counter()
        try:
            row = next(self.input)
            new_row = {}
            for expr, alias in zip(self.expressions, self.aliases):
                val = self.evaluate(expr, row)
                name = alias if alias else self._get_default_name(expr)
                new_row[name] = val
            self.stats["processed_rows"] += 1
            return new_row
        finally:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000

    def _get_default_name(self, expr):
        if isinstance(expr, ColumnRef): return expr.name
        return str(expr)

class PhysicalJoin(PhysicalOperator):
    def __init__(self, left: PhysicalOperator, right: PhysicalOperator, condition: Optional[BinaryExpression]):
        super().__init__()
        self.left = left
        self.right = right
        self.condition = condition
        self.left_row = None
        self.right_idx = 0
        self.right_rows = [] 
        self.right_exhausted = False

    def __next__(self) -> Dict[str, Any]:
        start = time.perf_counter()
        try:
            # Materialize right side only once
            if not self.right_rows and not self.right_exhausted:
                try:
                    while True:
                        self.right_rows.append(next(self.right))
                except StopIteration:
                    self.right_exhausted = True

            if self.left_row is None:
                self.left_row = next(self.left)
                self.right_idx = 0

            while True:
                if self.right_idx < len(self.right_rows):
                    r_row = self.right_rows[self.right_idx]
                    self.right_idx += 1
                    
                    # Zero-allocation condition check
                    if self.condition is None or self.evaluate(self.condition, self.left_row, r_row):
                        # Only merge on success
                        combined = {**self.left_row, **r_row}
                        self.stats["processed_rows"] += 1
                        return combined
                else:
                    self.left_row = next(self.left)
                    self.right_idx = 0
        finally:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000

class PhysicalHashAggregate(PhysicalOperator):
    def __init__(self, input: PhysicalOperator, group_by: List[str], aggregates: List[AggregateExpression]):
        super().__init__()
        self.input = input
        self.group_by = group_by
        self.aggregates = aggregates
        self.groups: Dict[Tuple, Dict[str, Any]] = {}
        self.result_rows = []
        self.cursor = 0
        self.executed = False

    def __next__(self) -> Dict[str, Any]:
        start = time.perf_counter()
        try:
            if not self.executed:
                self._execute()
            
            if self.cursor < len(self.result_rows):
                row = self.result_rows[self.cursor]
                self.cursor += 1
                return row
            raise StopIteration
        finally:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000

    def _execute(self):
        try:
            while True:
                row = next(self.input)
                key = []
                for g_col in self.group_by:
                    val = self._find_in_row(g_col, row)
                    key.append(val)
                key = tuple(key)

                if key not in self.groups:
                    self.groups[key] = {f"{agg.func}({agg.expression.pretty()})": self._init_agg(agg) for agg in self.aggregates}
                    for i, g_col in enumerate(self.group_by):
                        full_name = self._find_full_name(g_col, row)
                        self.groups[key][full_name] = key[i]
                
                for agg in self.aggregates:
                    agg_key = f"{agg.func}({agg.expression.pretty()})"
                    val = self.evaluate(agg.expression, row)
                    self.groups[key][agg_key] = self._update_agg(agg, self.groups[key][agg_key], val)
        except StopIteration: pass

        for key, agg_results in self.groups.items():
            for agg in self.aggregates:
                if agg.func == "AVG":
                    agg_key = f"{agg.func}({agg.expression.pretty()})"
                    s, c = agg_results[agg_key]
                    agg_results[agg_key] = s / c if c > 0 else 0
            self.result_rows.append(agg_results)
            self.stats["processed_rows"] += 1
        self.executed = True

    def _find_full_name(self, col_name, row):
        if col_name in row: return col_name
        for k in row.keys():
            if k.endswith(f".{col_name}"): return k
        return col_name

    def _find_in_row(self, col_name, row):
        if col_name in row: return row[col_name]
        for k, v in row.items():
            if k.endswith(f".{col_name}"): return v
        return None

    def _init_agg(self, agg: AggregateExpression):
        if agg.func == "COUNT": return 0
        if agg.func == "SUM": return 0
        if agg.func == "MIN": return float('inf')
        if agg.func == "MAX": return float('-inf')
        if agg.func == "AVG": return [0, 0]
        return None

    def _update_agg(self, agg: AggregateExpression, current, val):
        if val is None: return current
        if agg.func == "COUNT": return current + 1
        if agg.func == "SUM": return current + val
        if agg.func == "MIN": return min(current, val)
        if agg.func == "MAX": return max(current, val)
        if agg.func == "AVG":
            current[0] += val
            current[1] += 1
            return current
        return current

class PhysicalIndexScan(PhysicalOperator):
    def __init__(self, table_name: str, index_column: str, value: Any, columns: List[str], file_path: str, alias: Optional[str] = None):
        super().__init__()
        self.table_name = table_name
        self.index_column = index_column
        self.value = value
        self.columns = columns
        self.file_path = file_path
        self.alias = alias or table_name
        self.index: Dict[Any, List[Dict[str, Any]]] = {}
        self.loaded = False
        self.cursor = 0
        self.matches = []

    def __next__(self) -> Dict[str, Any]:
        start = time.perf_counter()
        try:
            if not self.loaded: self._build_index()
            if self.cursor < len(self.matches):
                res = self.matches[self.cursor]
                self.cursor += 1
                self.stats["processed_rows"] += 1
                return res
            raise StopIteration
        finally:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000

    def _build_index(self):
        if not os.path.exists(self.file_path): raise FileNotFoundError(self.file_path)
        with open(self.file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                prefixed_row = {f"{self.alias}.{k}": self._parse_val(v) for k, v in row.items() if k in self.columns}
                idx_val = self._parse_val(row[self.index_column])
                if idx_val not in self.index: self.index[idx_val] = []
                self.index[idx_val].append(prefixed_row)
        self.matches = self.index.get(self.value, [])
        self.loaded = True

    def _parse_val(self, v):
        try:
            if '.' in v: return float(v)
            return int(v)
        except ValueError: return v

class PhysicalLimit(PhysicalOperator):
    def __init__(self, input: PhysicalOperator, limit: int):
        super().__init__()
        self.input = input
        self.limit = limit
        self.cursor = 0

    def __next__(self) -> Dict[str, Any]:
        start = time.perf_counter()
        try:
            if self.cursor >= self.limit:
                raise StopIteration
            
            row = next(self.input)
            self.cursor += 1
            self.stats["processed_rows"] += 1
            return row
        finally:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000
