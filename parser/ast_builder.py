from lark import Transformer, v_args
from ast_nodes.nodes import (
    SelectStatement, SelectItem, TableRef, JoinClause, 
    ColumnRef, Literal, BinaryExpression, LogicalExpression
)

class SQLTransformer(Transformer):
    def select_statement(self, children):
        select_items = children[0]
        from_table = children[1]
        joins = []
        where = None
        
        for child in children[2:]:
            if isinstance(child, JoinClause):
                joins.append(child)
            elif isinstance(child, (BinaryExpression, LogicalExpression, ColumnRef, Literal)):
                where = child
        
        return SelectStatement(select_items=select_items, from_table=from_table, joins=joins, where=where)

    def select_items(self, children):
        return children

    def select_item(self, children):
        expr = children[0]
        alias = children[1] if len(children) > 1 else None
        return SelectItem(expression=expr, alias=alias)

    def table_ref(self, children):
        name = str(children[0])
        alias = str(children[1]) if len(children) > 1 else None
        return TableRef(table_name=name, alias=alias)

    def join_clause(self, children):
        join_type_str = str(children[0])
        table = children[1]
        condition = children[2]
        return JoinClause(table=table, join_type=join_type_str, condition=condition)

    def inner_join(self, _):
        return "INNER JOIN"

    def left_join(self, _):
        return "LEFT JOIN"

    def where_clause(self, children):
        return children[0]

    def logical_or(self, children):
        if len(children) == 1:
            return children[0]
        res = children[0]
        for i in range(1, len(children)):
            res = LogicalExpression(left=res, operator="OR", right=children[i])
        return res

    def logical_and(self, children):
        if len(children) == 1:
            return children[0]
        res = children[0]
        for i in range(1, len(children)):
            res = LogicalExpression(left=res, operator="AND", right=children[i])
        return res

    def comparison(self, children):
        if len(children) == 1:
            return children[0]
        return BinaryExpression(left=children[0], operator=str(children[1]), right=children[2])

    def column_ref(self, children):
        if len(children) == 2:
            return ColumnRef(table=str(children[0]), name=str(children[1]))
        return ColumnRef(name=str(children[0]))

    def number(self, children):
        val = str(children[0])
        if "." in val:
            return Literal(value=float(val))
        return Literal(value=int(val))

    def string(self, children):
        # Remove quotes
        val = str(children[0])[1:-1]
        return Literal(value=val)

    def identifier(self, children):
        return str(children[0])
