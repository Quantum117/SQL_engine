import numpy as np
import csv
import os
import time
from typing import Dict, Any, List, Optional
from ast_nodes.nodes import ColumnRef, Literal, BinaryExpression, LogicalExpression, AggregateExpression

class VectorizedOperator:
    def __init__(self):
        self.stats = {"processed_rows": 0, "time_ms": 0.0}

    def next_batch(self, batch_size: int = 1024) -> Optional[Dict[str, np.ndarray]]:
        raise NotImplementedError

    def get_stats(self) -> Dict[str, int]:
        return self.stats

class VectorizedScan(VectorizedOperator):
    def __init__(self, table_name: str, columns: List[str], file_path: str, alias: Optional[str] = None):
        super().__init__()
        self.table_name = table_name
        self.columns = columns
        self.file_path = file_path
        self.alias = alias or table_name
        self.data: Dict[str, np.ndarray] = {}
        self.loaded = False
        self.cursor = 0

    def _load(self, batch_size):
        import pandas as pd
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
        
        self.chunk_iter = pd.read_csv(self.file_path, chunksize=batch_size)
        self.loaded = True

    def next_batch(self, batch_size: int = 1024) -> Optional[Dict[str, np.ndarray]]:
        start = time.perf_counter()
        if not self.loaded:
            self._load(batch_size)

        try:
            df = next(self.chunk_iter)
            # Ensure it's a numpy array per column
            batch = {f"{self.alias}.{col}": df[col].values for col in self.columns}
            self.stats["processed_rows"] += len(df)
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000
            return batch
        except StopIteration:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000
            return None

class VectorizedFilter(VectorizedOperator):
    def __init__(self, input: VectorizedOperator, condition: Any):
        super().__init__()
        self.input = input
        self.condition = condition

    def next_batch(self, batch_size: int = 1024) -> Optional[Dict[str, np.ndarray]]:
        start = time.perf_counter()
        while True:
            batch = self.input.next_batch(batch_size)
            if batch is None: 
                self.stats["time_ms"] += (time.perf_counter() - start) * 1000
                return None

            mask = self.evaluate(self.condition, batch)
            if np.any(mask):
                filtered_batch = {k: v[mask] for k, v in batch.items()}
                self.stats["processed_rows"] += np.sum(mask)
                self.stats["time_ms"] += (time.perf_counter() - start) * 1000
                return filtered_batch

    def evaluate(self, expr, batch) -> np.ndarray:
        if isinstance(expr, Literal):
            return expr.value
        if isinstance(expr, ColumnRef):
            qual_name = f"{expr.table}.{expr.name}"
            if qual_name in batch: return batch[qual_name]
            if expr.name in batch: return batch[expr.name]
            raise KeyError(f"Column {qual_name} or {expr.name} not found in batch keys: {list(batch.keys())}")
        
        if isinstance(expr, AggregateExpression):
            key = f"{expr.func}({expr.expression.pretty()})"
            return batch[key]

        if isinstance(expr, BinaryExpression):
            left = self.evaluate(expr.left, batch)
            right = self.evaluate(expr.right, batch)
            op = expr.operator
            if op == '=': return left == right
            if op == '!=': return left != right
            if op == '>': return left > right
            if op == '<': return left < right
            if op == '>=': return left >= right
            if op == '<=': return left <= right
        
        if isinstance(expr, LogicalExpression):
            left = self.evaluate(expr.left, batch)
            right = self.evaluate(expr.right, batch)
            if expr.operator == 'AND': return np.logical_and(left, right)
            if expr.operator == 'OR': return np.logical_or(left, right)
        
        return np.ones(len(next(iter(batch.values()))), dtype=bool)

class VectorizedProject(VectorizedOperator):
    def __init__(self, input: VectorizedOperator, expressions: List[Any], aliases: List[Optional[str]]):
        super().__init__()
        self.input = input
        self.expressions = expressions
        self.aliases = aliases

    def next_batch(self, batch_size: int = 1024) -> Optional[Dict[str, np.ndarray]]:
        start = time.perf_counter()
        batch = self.input.next_batch(batch_size)
        if batch is None: 
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000
            return None

        new_batch = {}
        for expr, alias in zip(self.expressions, self.aliases):
            val = self.evaluate(expr, batch)
            name = alias if alias else self._get_default_name(expr)
            new_batch[name] = val
        
        self.stats["processed_rows"] += len(next(iter(new_batch.values())))
        self.stats["time_ms"] += (time.perf_counter() - start) * 1000
        return new_batch

    def _get_default_name(self, expr):
        if isinstance(expr, ColumnRef): return expr.name
        return str(expr)

    def evaluate(self, expr, batch):
        v_filter = VectorizedFilter(None, None)
        return v_filter.evaluate(expr, batch)

class VectorizedHashJoin(VectorizedOperator):
    def __init__(self, left: VectorizedOperator, right: VectorizedOperator, condition: BinaryExpression):
        super().__init__()
        self.left = left
        self.right = right
        self.condition = condition
        self.hash_table = {}
        self.build_finished = False
        self.left_batch = None
        self.left_cursor = 0

    def _build(self):
        # We assume for now it's an equality on columns.
        all_right_data = []
        while True:
            batch = self.right.next_batch(4096)
            if batch is None: break
            all_right_data.append(batch)
        
        if not all_right_data:
            self.build_finished = True
            return

        self.full_right = {k: np.concatenate([b[k] for b in all_right_data]) for k in all_right_data[0].keys()}
        
        # Find the right key and Sort it for Vectorized Binary Search
        right_col = self.condition.right
        qual_name = f"{right_col.table}.{right_col.name}"
        right_keys = self.full_right[qual_name] if qual_name in self.full_right else self.full_right[right_col.name]
        
        # Sort keys to enable np.searchsorted (Sort-Merge / Binary-Search join)
        self.sorted_indices = np.argsort(right_keys)
        self.sorted_keys = right_keys[self.sorted_indices]
        
        self.build_finished = True

    def next_batch(self, batch_size: int = 1024) -> Optional[Dict[str, np.ndarray]]:
        start = time.perf_counter()
        if not self.build_finished:
            self._build()

        left_col = self.condition.left
        
        if self.left_batch is None:
            self.left_batch = self.left.next_batch(batch_size)
            if self.left_batch is None: 
                self.stats["time_ms"] += (time.perf_counter() - start) * 1000
                return None
        
        qual_name = f"{left_col.table}.{left_col.name}"
        left_keys = self.left_batch[qual_name] if qual_name in self.left_batch else self.left_batch[left_col.name]
        
        # Vectorized lookup using NumPy instead of Python loop
        # searchsorted finds the insertion point, which is our match index if it exists
        indices = np.searchsorted(self.sorted_keys, left_keys)
        
        # Create a mask for valid matches (must be within bounds and equal value)
        valid_mask = (indices < len(self.sorted_keys))
        # Safely index into sorted keys to check for exact match
        matched_indices = indices[valid_mask]
        match_equality = (self.sorted_keys[matched_indices] == left_keys[valid_mask])
        
        # Combined mask of left side records that have a match
        left_match_mask = valid_mask.copy()
        left_match_mask[valid_mask] = match_equality
        
        # Final set of indices on both sides
        left_indices = np.where(left_match_mask)[0]
        right_indices = self.sorted_indices[indices[left_match_mask]]
        
        res = {}
        for k, v in self.left_batch.items():
            res[k] = v[left_indices]
        for k, v in self.full_right.items():
            res[k] = v[right_indices]
            
        self.stats["processed_rows"] += len(left_indices)
        self.left_batch = None # Simple 1batch:1result for simplified scaling
        res = res if len(left_indices) > 0 else self.next_batch(batch_size)
        self.stats["time_ms"] += (time.perf_counter() - start) * 1000
        return res

class VectorizedHashAggregate(VectorizedOperator):
    def __init__(self, input: VectorizedOperator, group_by: List[str], aggregates: List[AggregateExpression]):
        super().__init__()
        self.input = input
        self.group_by = group_by
        self.aggregates = aggregates
        self.result_batch = None
        self.executed = False

    def next_batch(self, batch_size: int = 1024) -> Optional[Dict[str, np.ndarray]]:
        start = time.perf_counter()
        if not self.executed:
            self._execute()
        if self.result_batch is None: 
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000
            return None
        res = self.result_batch
        self.result_batch = None
        self.executed = True
        self.stats["time_ms"] += (time.perf_counter() - start) * 1000
        return res

    def _execute(self):
        all_batches = []
        while True:
            batch = self.input.next_batch(4096)
            if batch is None: break
            all_batches.append(batch)
        if not all_batches: return

        full_data = {k: np.concatenate([b[k] for b in all_batches]) for k in all_batches[0].keys()}
        
        if not self.group_by:
            res = {}
            for agg in self.aggregates:
                agg_key = f"{agg.func}({agg.expression.pretty()})"
                res[agg_key] = np.array([self._run_agg(agg, full_data)])
            self.result_batch = res
            return

        group_cols = [self._find_in_batch(g, full_data) for g in self.group_by]
        
        if len(group_cols) == 1:
            unique_keys, inverse = np.unique(group_cols[0], return_inverse=True)
        else:
            combined = np.array([str(row) for row in zip(*group_cols)])
            unique_keys, inverse = np.unique(combined, return_inverse=True)

        res = {}
        if len(group_cols) == 1:
            full_name = self._find_full_name(self.group_by[0], full_data)
            res[full_name] = unique_keys
        else:
            for i, g_col in enumerate(self.group_by):
                 group_reps = [group_cols[i][np.where(inverse == g_idx)[0][0]] for g_idx in range(len(unique_keys))]
                 full_name = self._find_full_name(g_col, full_data)
                 res[full_name] = np.array(group_reps)

        for agg in self.aggregates:
            agg_key = f"{agg.func}({agg.expression.pretty()})"
            expr_data = self.evaluate(agg.expression, full_data)
            reduced = np.zeros(len(unique_keys))
            if agg.func == "COUNT": np.add.at(reduced, inverse, 1)
            elif agg.func == "SUM": np.add.at(reduced, inverse, expr_data)
            elif agg.func == "MIN":
                reduced.fill(np.inf)
                for i, val in enumerate(expr_data): reduced[inverse[i]] = min(reduced[inverse[i]], val)
            elif agg.func == "MAX":
                reduced.fill(-np.inf)
                for i, val in enumerate(expr_data): reduced[inverse[i]] = max(reduced[inverse[i]], val)
            elif agg.func == "AVG":
                sums, counts = np.zeros(len(unique_keys)), np.zeros(len(unique_keys))
                np.add.at(sums, inverse, expr_data)
                np.add.at(counts, inverse, 1)
                reduced = sums / counts
            res[agg_key] = reduced

        self.result_batch = res
        self.stats["processed_rows"] += len(unique_keys)

    def _run_agg(self, agg, data):
        expr_data = self.evaluate(agg.expression, data)
        if agg.func == "COUNT": return len(expr_data)
        if agg.func == "SUM": return np.sum(expr_data)
        if agg.func == "MIN": return np.min(expr_data)
        if agg.func == "MAX": return np.max(expr_data)
        if agg.func == "AVG": return np.mean(expr_data)
        return None

    def _find_full_name(self, col_name, batch):
        if col_name in batch: return col_name
        for k in batch.keys():
            if k.endswith(f".{col_name}"): return k
        return col_name

    def _find_in_batch(self, col_name, batch):
        if col_name in batch: return batch[col_name]
        for k, v in batch.items():
            if k.endswith(f".{col_name}"): return v
        return None

    def evaluate(self, expr, batch):
        v_filter = VectorizedFilter(None, None)
        return v_filter.evaluate(expr, batch)

class VectorizedIndexScan(VectorizedOperator):
    def __init__(self, table_name: str, index_column: str, value: Any, columns: List[str], file_path: str, alias: Optional[str] = None):
        super().__init__()
        self.table_name = table_name
        self.index_column = index_column
        self.value = value
        self.columns = columns
        self.file_path = file_path
        self.alias = alias or table_name
        self.index: Dict[Any, np.ndarray] = {} # Val -> indices
        self.loaded = False
        self.cursor = 0
        self.match_indices = None
        self.full_data = {}

    def _load(self):
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
        
        raw_data = np.genfromtxt(self.file_path, delimiter=',', names=True, dtype=None, encoding='utf-8')
        
        # Build index
        idx_col_data = raw_data[self.index_column]
        unique_vals = np.unique(idx_col_data)
        for val in unique_vals:
            self.index[val] = np.where(idx_col_data == val)[0]
        
        for col in self.columns:
            key = f"{self.alias}.{col}"
            self.full_data[key] = raw_data[col]
        
        self.match_indices = self.index.get(self.value, np.array([], dtype=int))
        self.row_count = len(self.match_indices)
        self.loaded = True

    def next_batch(self, batch_size: int = 1024) -> Optional[Dict[str, np.ndarray]]:
        start = time.perf_counter()
        if not self.loaded:
            self._load()

        if self.cursor >= self.row_count:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000
            return None

        end = min(self.cursor + batch_size, self.row_count)
        indices = self.match_indices[self.cursor:end]
        
        batch = {k: v[indices] for k, v in self.full_data.items()}
        self.stats["processed_rows"] += len(indices)
        self.cursor = end
        self.stats["time_ms"] += (time.perf_counter() - start) * 1000
        return batch

class VectorizedLimit(VectorizedOperator):
    def __init__(self, input: VectorizedOperator, limit: int):
        super().__init__()
        self.input = input
        self.limit = limit
        self.cursor = 0

    def next_batch(self, batch_size: int = 1024) -> Optional[Dict[str, np.ndarray]]:
        start = time.perf_counter()
        if self.cursor >= self.limit:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000
            return None

        batch = self.input.next_batch(batch_size)
        if batch is None:
            self.stats["time_ms"] += (time.perf_counter() - start) * 1000
            return None
        
        # Calculate how many rows we can still take
        remaining = self.limit - self.cursor
        
        # We assume all columns in batch have same length
        batch_len = len(next(iter(batch.values())))
        
        if batch_len > remaining:
            # Slice down to the remaining quota
            batch = {k: v[:remaining] for k, v in batch.items()}
            batch_len = remaining

        self.cursor += batch_len
        self.stats["processed_rows"] += batch_len
        self.stats["time_ms"] += (time.perf_counter() - start) * 1000
        return batch
