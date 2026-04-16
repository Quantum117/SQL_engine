from .logical_operators import LogicalOperator, LogicalScan, LogicalFilter, LogicalProject, LogicalJoin, LogicalAggregate, LogicalIndexScan
from ast_nodes.nodes import BinaryExpression, LogicalExpression, ColumnRef, Literal
from catalog.catalog import Catalog
from typing import Any, Set, List, Optional

class OptimizationRule:
    def apply(self, op: LogicalOperator, catalog: Catalog) -> LogicalOperator:
        raise NotImplementedError

    def _get_referenced_columns(self, expr) -> Set[str]:
        cols = set()
        if isinstance(expr, ColumnRef):
            if expr.table:
                cols.add(f"{expr.table}.{expr.name}")
            else:
                cols.add(expr.name)
        elif isinstance(expr, BinaryExpression):
            cols.update(self._get_referenced_columns(expr.left))
            cols.update(self._get_referenced_columns(expr.right))
        elif isinstance(expr, LogicalExpression):
            cols.update(self._get_referenced_columns(expr.left))
            cols.update(self._get_referenced_columns(expr.right))
        return cols

class ConstantFoldingRule(OptimizationRule):
    def apply(self, op: LogicalOperator, catalog: Catalog) -> LogicalOperator:
        if isinstance(op, LogicalFilter):
            op.input = self.apply(op.input, catalog)
            op.condition = self._fold(op.condition)
        elif hasattr(op, 'input'):
            op.input = self.apply(op.input, catalog)
        elif isinstance(op, LogicalJoin):
            op.left = self.apply(op.left, catalog)
            op.right = self.apply(op.right, catalog)
        elif isinstance(op, LogicalProject):
            op.input = self.apply(op.input, catalog)
        elif isinstance(op, LogicalAggregate):
            op.input = self.apply(op.input, catalog)
        return op

    def _fold(self, expr):
        if isinstance(expr, BinaryExpression):
            left = self._fold(expr.left)
            right = self._fold(expr.right)
            if isinstance(left, Literal) and isinstance(right, Literal):
                if isinstance(left.value, (int, float)) and isinstance(right.value, (int, float)):
                    if expr.operator == '+': return Literal(left.value + right.value)
                    if expr.operator == '-': return Literal(left.value - right.value)
            return BinaryExpression(left, expr.operator, right)
        return expr

class PredicatePushdownRule(OptimizationRule):
    def apply(self, op: LogicalOperator, catalog: Catalog) -> LogicalOperator:
        if isinstance(op, LogicalFilter):
            conditions = self._split_conditions(op.condition)
            current_node = self.apply(op.input, catalog)
            remaining_conditions = []

            for cond in conditions:
                pushed = self._push_down(cond, current_node, catalog)
                if pushed:
                    current_node = pushed
                else:
                    remaining_conditions.append(cond)
            
            if remaining_conditions:
                combined_cond = remaining_conditions[0]
                for next_cond in remaining_conditions[1:]:
                    combined_cond = LogicalExpression(combined_cond, "AND", next_cond)
                return LogicalFilter(current_node, combined_cond)
            return current_node
            
        if hasattr(op, 'input'):
            op.input = self.apply(op.input, catalog)
        elif isinstance(op, LogicalJoin):
            op.left = self.apply(op.left, catalog)
            op.right = self.apply(op.right, catalog)
        return op

    def _split_conditions(self, expr) -> List[Any]:
        if isinstance(expr, LogicalExpression) and expr.operator == "AND":
            return self._split_conditions(expr.left) + self._split_conditions(expr.right)
        return [expr]

    def _push_down(self, cond, node, catalog) -> Optional[LogicalOperator]:
        if isinstance(node, LogicalJoin):
            cond_cols = self._get_referenced_columns(cond)
            left_cols = self._get_available_columns(node.left)
            right_cols = self._get_available_columns(node.right)

            # Check for subset of table-qualified names
            if cond_cols.issubset(left_cols):
                new_left = self._push_down(cond, node.left, catalog) or LogicalFilter(node.left, cond)
                return LogicalJoin(new_left, node.right, node.join_type, node.condition)
            elif cond_cols.issubset(right_cols):
                new_right = self._push_down(cond, node.right, catalog) or LogicalFilter(node.right, cond)
                return LogicalJoin(node.left, new_right, node.join_type, node.condition)
        
        return None


    def _get_available_columns(self, op: LogicalOperator) -> Set[str]:
        cols = set()
        if isinstance(op, LogicalScan):
            alias = op.alias or op.table_name
            for c in op.columns:
                cols.add(f"{alias}.{c}")
                cols.add(c) # Also add unqualified for safety
        elif isinstance(op, LogicalFilter):
            cols.update(self._get_available_columns(op.input))
        elif isinstance(op, LogicalJoin):
            cols.update(self._get_available_columns(op.left))
            cols.update(self._get_available_columns(op.right))
        elif isinstance(op, LogicalProject):
            for expr in op.expressions:
                if isinstance(expr, ColumnRef):
                     if expr.table: cols.add(f"{expr.table}.{expr.name}")
                     cols.add(expr.name)
        elif isinstance(op, LogicalIndexScan):
            alias = op.alias or op.table_name
            for c in op.columns:
                cols.add(f"{alias}.{c}")
                cols.add(c)
        return cols

class ColumnPruningRule(OptimizationRule):
    def apply(self, op: LogicalOperator, catalog: Catalog) -> LogicalOperator:
        return self._prune(op, set())

    def _prune(self, op: LogicalOperator, required: Set[str]) -> LogicalOperator:
        if isinstance(op, LogicalProject):
            new_req = set()
            for expr in op.expressions:
                new_req.update(self._get_referenced_columns(expr))
            op.input = self._prune(op.input, new_req)
            return op
        
        elif isinstance(op, LogicalFilter):
            new_req = required.copy()
            new_req.update(self._get_referenced_columns(op.condition))
            op.input = self._prune(op.input, new_req)
            return op

        elif isinstance(op, LogicalAggregate):
            new_req = required.copy()
            for agg in op.aggregates:
                new_req.update(self._get_referenced_columns(agg.expression))
            for gb in op.group_by:
                new_req.add(gb)
            op.input = self._prune(op.input, new_req)
            return op

        elif isinstance(op, LogicalJoin):
            new_req = required.copy()
            if op.condition:
                new_req.update(self._get_referenced_columns(op.condition))
            op.left = self._prune(op.left, new_req)
            op.right = self._prune(op.right, new_req)
            return op

        elif isinstance(op, LogicalScan):
            alias = op.alias or op.table_name
            new_cols = []
            for col in op.columns:
                if col in required or f"{alias}.{col}" in required:
                    new_cols.append(col)
            # Never prune away everything, keep at least one column
            if not new_cols and op.columns:
                new_cols = [op.columns[0]]
            op.columns = new_cols
            return op
        
        elif hasattr(op, 'input'):
            op.input = self._prune(op.input, required)
        
        return op

class IndexRule(OptimizationRule):
    def apply(self, op: LogicalOperator, catalog: Catalog) -> LogicalOperator:
        if isinstance(op, LogicalFilter) and isinstance(op.input, LogicalScan):
            cond = op.condition
            scan = op.input
            if isinstance(cond, BinaryExpression) and cond.operator == '=':
                left, right = cond.left, cond.right
                col = None
                val = None
                if isinstance(left, ColumnRef) and isinstance(right, Literal):
                    col, val = left, right.value
                elif isinstance(right, ColumnRef) and isinstance(left, Literal):
                    col, val = right, left.value
                
                if col:
                    schema = catalog.get_table(scan.table_name)
                    if col.name in schema.indexes:
                        return LogicalIndexScan(scan.table_name, col.name, val, scan.columns, scan.alias)
        
        if hasattr(op, 'input'):
            op.input = self.apply(op.input, catalog)
        elif isinstance(op, LogicalJoin):
            op.left = self.apply(op.left, catalog)
            op.right = self.apply(op.right, catalog)
        
        return op

class LogicalOptimizer:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self.rules = []

    def add_rule(self, rule: OptimizationRule):
        self.rules.append(rule)

    def optimize(self, plan: LogicalOperator) -> LogicalOperator:
        for rule in self.rules:
            plan = rule.apply(plan, self.catalog)
        return plan
