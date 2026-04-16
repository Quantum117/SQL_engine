from lark import Transformer, v_args
from ast_nodes.nodes import (
    SelectStatement, SelectItem, TableRef, JoinClause, 
    ColumnRef, Literal, BinaryExpression, LogicalExpression, 
    AggregateExpression, SubqueryTableRef, ArithmeticExpression, OrderItem
)

class Clause:
    """Helper to tag clauses during transformation."""
    def __init__(self, type, val):
        self.type = type
        self.val = val

class SQLTransformer(Transformer):
    def select_items(self, children):
        return [c for c in children if c != ","]

    def select_item(self, children):
        expr = children[0]
        alias = None
        if len(children) > 1:
            alias = str(children[2]) if children[1] == "AS" else str(children[1])
        return SelectItem(expression=expr, alias=alias)

    def table_ref(self, children):
        if isinstance(children[0], SelectStatement):
            query = children[0]
            alias = str(children[1]) if len(children) > 1 else None
            return SubqueryTableRef(query=query, alias=alias)
        else:
            name = str(children[0])
            alias = str(children[1]) if len(children) > 1 else None
            return TableRef(table_name=name, alias=alias)

    def join_clause(self, children):
        real_children = [c for c in children if c not in ("JOIN", "ON")]
        if len(real_children) == 3:
            return JoinClause(join_type=real_children[0], table=real_children[1], condition=real_children[2])
        jt, tb, cond = "INNER JOIN", None, None
        for c in real_children:
            if isinstance(c, (TableRef, SubqueryTableRef)): tb = c
            elif isinstance(c, (BinaryExpression, LogicalExpression, ArithmeticExpression, ColumnRef, Literal, AggregateExpression)): cond = c
            elif isinstance(c, str) and "JOIN" in c: jt = c
        return JoinClause(join_type=jt, table=tb, condition=cond)

    def inner_join(self, _): return "INNER JOIN"
    def left_join(self, _): return "LEFT JOIN"

    def where_clause(self, children): return Clause("WHERE", children[1])
    def group_by_clause(self, children): return Clause("GROUP_BY", [c for c in children if isinstance(c, ColumnRef)])
    def having_clause(self, children): return Clause("HAVING", children[1])
    def order_by_clause(self, children): return Clause("ORDER_BY", [c for c in children if isinstance(c, OrderItem)])
    def limit_clause(self, children): return Clause("LIMIT", int(str(children[1])))

    def order_item(self, children):
        expr = children[0]
        direction = str(children[1]) if len(children) > 1 else "ASC"
        return OrderItem(expression=expr, direction=direction)

    def aggregate_expression(self, children):
        return AggregateExpression(func=str(children[0]), expression=children[1])

    def logical_or(self, children):
        if len(children) == 1: return children[0]
        return LogicalExpression(left=children[0], operator="OR", right=children[2])

    def logical_and(self, children):
        if len(children) == 1: return children[0]
        return LogicalExpression(left=children[0], operator="AND", right=children[2])

    def comparison(self, children):
        if len(children) == 1: return children[0]
        return BinaryExpression(left=children[0], operator=str(children[1]), right=children[2])

    def arithmetic_expr(self, children):
        if len(children) == 1: return children[0]
        return ArithmeticExpression(left=children[0], operator=str(children[1]), right=children[2])

    def term(self, children):
        if len(children) == 1: return children[0]
        return ArithmeticExpression(left=children[0], operator=str(children[1]), right=children[2])

    def column_ref(self, children):
        real = [c for c in children if c != "."]
        if len(real) == 2: return ColumnRef(table=str(real[0]), name=str(real[1]))
        return ColumnRef(table=None, name=str(real[0]))

    @v_args(inline=True)
    def number(self, n): return Literal(value=float(n) if '.' in n else int(n))
    @v_args(inline=True)
    def string(self, s): return Literal(value=s[1:-1])
    def identifier(self, children): return str(children[0])

    def select_statement(self, children):
        res = {'select_items': [], 'from_table': None, 'joins': [], 'where': None, 'group_by': [], 'having': None, 'order_by': [], 'limit': None}
        for c in children:
            if c == "SELECT" or c == "FROM" or c == ";": continue
            if isinstance(c, list): res['select_items'] = c
            elif isinstance(c, (TableRef, SubqueryTableRef)): res['from_table'] = c
            elif isinstance(c, JoinClause): res['joins'].append(c)
            elif isinstance(c, Clause):
                if c.type == "WHERE": res['where'] = c.val
                elif c.type == "GROUP_BY": res['group_by'] = c.val
                elif c.type == "HAVING": res['having'] = c.val
                elif c.type == "ORDER_BY": res['order_by'] = c.val
                elif c.type == "LIMIT": res['limit'] = c.val
        return SelectStatement(**res)
