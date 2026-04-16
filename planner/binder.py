from ast_nodes.nodes import ASTNode, SelectStatement, TableRef, ColumnRef, BinaryExpression, LogicalExpression, Literal, AggregateExpression, SubqueryTableRef
from .logical_operators import LogicalScan, LogicalFilter, LogicalProject, LogicalJoin, LogicalAggregate, LogicalLimit
from catalog.catalog import Catalog, TableSchema
from typing import Dict, List, Optional, Tuple, Any

class SemanticError(Exception):
    pass

class Binder:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self.current_scope: Dict[str, TableSchema] = {} # Alias -> Schema

    def bind_select(self, stmt: SelectStatement) -> Tuple[Any, List[str]]:
        # Store old scope for recursion/subqueries if needed
        old_scope = self.current_scope.copy()
        
        # 1. Bind FROM
        source = self.bind_table(stmt.from_table)
        
        # 2. Bind JOINS
        for join in stmt.joins:
            right_source = self.bind_table(join.table)
            cond = self.bind_expression(join.condition) if join.condition else None
            source = LogicalJoin(source, right_source, join.join_type, cond)

        # 3. Bind WHERE
        if stmt.where:
            cond = self.bind_expression(stmt.where)
            source = LogicalFilter(source, cond)

        # 4. Check for Aggregations
        has_aggregates = any(self._contains_aggregate(item.expression) for item in stmt.select_items)
        
        # group_by is now List[ColumnRef], resolve and extract names for compatibility
        group_by_nodes = [self.bind_expression(gb) for gb in stmt.group_by]
        group_by_names = [gb.name for gb in group_by_nodes]
        
        if has_aggregates or group_by_names:
            # Bind Aggregation
            aggs = []
            for item in stmt.select_items:
                if self._contains_aggregate(item.expression):
                    aggs.append(self.bind_expression(item.expression))
            
            # Semantic check: non-aggregated columns in SELECT must be in GROUP BY
            for item in stmt.select_items:
                if not self._contains_aggregate(item.expression):
                    if not isinstance(item.expression, ColumnRef):
                         raise SemanticError(f"Select item '{item.expression.pretty()}' must be an aggregate or in GROUP BY")
                    
                    # Resolved column check
                    resolved_col = self.bind_expression(item.expression)
                    if resolved_col.name not in group_by_names:
                         raise SemanticError(f"Column '{resolved_col.name}' must appear in the GROUP BY clause or be used in an aggregate function")
            
            source = LogicalAggregate(source, group_by_names, aggs)

        # 5. Bind SELECT items (Project on top of Aggregate or Source)
        exprs = []
        aliases = []
        for item in stmt.select_items:
            if isinstance(item.expression, ColumnRef) and item.expression.name == "*":
                if item.expression.table:
                    if item.expression.table not in self.current_scope:
                        raise SemanticError(f"Table alias '{item.expression.table}' not found in scope for * expansion")
                    schema = self.current_scope[item.expression.table]
                    for col in schema.columns:
                        exprs.append(ColumnRef(table=item.expression.table, name=col))
                        aliases.append(None)
                else:
                    for alias, schema in self.current_scope.items():
                        for col in schema.columns:
                            exprs.append(ColumnRef(table=alias, name=col))
                            aliases.append(None)
            else:
                expr = self.bind_expression(item.expression)
                exprs.append(expr)
                aliases.append(item.alias)

        res_plan = LogicalProject(source, exprs, aliases)
        if stmt.limit is not None:
            res_plan = LogicalLimit(res_plan, stmt.limit)
            
        self.current_scope = old_scope
        return res_plan, []

    def _contains_aggregate(self, expr: ASTNode) -> bool:
        if isinstance(expr, AggregateExpression):
            return True
        if isinstance(expr, (BinaryExpression, LogicalExpression)):
            return self._contains_aggregate(expr.left) or self._contains_aggregate(expr.right)
        return False

    def bind_table(self, table_ref: Any) -> Any:
        if isinstance(table_ref, TableRef):
            schema = self.catalog.get_table(table_ref.table_name)
            alias = table_ref.alias or table_ref.table_name
            if alias in self.current_scope:
                raise SemanticError(f"Duplicate table alias: {alias}")
            self.current_scope[alias] = schema
            return LogicalScan(table_ref.table_name, list(schema.columns), alias)
        
        elif isinstance(table_ref, SubqueryTableRef):
            # Recursively bind the subquery
            inner_binder = Binder(self.catalog)
            inner_plan, _ = inner_binder.bind_select(table_ref.query)
            
            if not isinstance(inner_plan, LogicalProject):
                raise SemanticError("Subquery must return a projectable result")
            
            # Create a virtual schema for the subquery
            inner_cols = []
            for expr, alias in zip(inner_plan.expressions, inner_plan.aliases):
                name = self._get_export_name(expr, alias)
                inner_cols.append(name)
            
            subquery_alias = table_ref.alias or f"subquery_{id(table_ref)}"
            virtual_schema = TableSchema.from_lists(subquery_alias, inner_cols, ["ANY"]*len(inner_cols))
            
            if subquery_alias in self.current_scope:
                raise SemanticError(f"Duplicate table alias: {subquery_alias}")
            self.current_scope[subquery_alias] = virtual_schema
            
            return inner_plan
        
        raise NotImplementedError(f"Binding for {type(table_ref)} not implemented")

    def _get_export_name(self, expr: Any, alias: Optional[str]) -> str:
        if alias:
            return alias
        if isinstance(expr, ColumnRef):
            return expr.name
        if isinstance(expr, AggregateExpression):
            return f"{expr.func}({self._get_export_name(expr.expression, None)})"
        return str(expr)

    def bind_expression(self, expr: ASTNode) -> Any:
        if isinstance(expr, ColumnRef):
            return self.resolve_column(expr)
        elif isinstance(expr, BinaryExpression):
            return BinaryExpression(
                left=self.bind_expression(expr.left),
                operator=expr.operator,
                right=self.bind_expression(expr.right)
            )
        elif isinstance(expr, LogicalExpression):
            return LogicalExpression(
                left=self.bind_expression(expr.left),
                operator=expr.operator,
                right=self.bind_expression(expr.right)
            )
        elif isinstance(expr, AggregateExpression):
            return AggregateExpression(
                func=expr.func,
                expression=self.bind_expression(expr.expression)
            )
        elif isinstance(expr, Literal):
            return expr
        return expr

    def resolve_column(self, col: ColumnRef) -> ColumnRef:
        if col.table:
            if col.table not in self.current_scope:
                raise SemanticError(f"Table alias '{col.table}' not found in scope")
            schema = self.current_scope[col.table]
            if col.name not in schema.columns:
                 raise SemanticError(f"Column '{col.name}' not found in table '{col.table}'")
            return ColumnRef(table=col.table, name=col.name)
        else:
            # Search in all tables in scope
            matches = []
            for alias, schema in self.current_scope.items():
                if col.name in schema.columns:
                    matches.append(alias)
            
            if not matches:
                raise SemanticError(f"Column '{col.name}' not found in any table")
            if len(matches) > 1:
                raise SemanticError(f"Column '{col.name}' is ambiguous (matches: {', '.join(matches)})")
            
            return ColumnRef(table=matches[0], name=col.name)
