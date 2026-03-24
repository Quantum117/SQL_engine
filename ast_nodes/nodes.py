from dataclasses import dataclass, field
from typing import List, Optional, Union

@dataclass
class ASTNode:
    def pretty(self, indent: int = 0) -> str:
        raise NotImplementedError

    def to_dot(self, dot, parent_id: Optional[str] = None):
        raise NotImplementedError

@dataclass
class Literal(ASTNode):
    value: Union[int, float, str]

    def pretty(self, indent: int = 0) -> str:
        if isinstance(self.value, str):
            return f"'{self.value}'"
        return str(self.value)

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        label = f"Literal: {self.pretty()}"
        dot.node(node_id, label)
        if parent_id:
            dot.edge(parent_id, node_id)

@dataclass
class ColumnRef(ASTNode):
    name: str
    table: Optional[str] = None

    def pretty(self, indent: int = 0) -> str:
        if self.table:
            return f"{self.table}.{self.name}"
        return self.name

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        label = f"Column: {self.pretty()}"
        dot.node(node_id, label)
        if parent_id:
            dot.edge(parent_id, node_id)

@dataclass
class BinaryExpression(ASTNode):
    left: ASTNode
    operator: str
    right: ASTNode

    def pretty(self, indent: int = 0) -> str:
        return f"({self.left.pretty()} {self.operator} {self.right.pretty()})"

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        dot.node(node_id, f"BinaryExpr: {self.operator}")
        if parent_id:
            dot.edge(parent_id, node_id)
        self.left.to_dot(dot, node_id)
        self.right.to_dot(dot, node_id)

@dataclass
class LogicalExpression(ASTNode):
    left: ASTNode
    operator: str
    right: ASTNode

    def pretty(self, indent: int = 0) -> str:
        return f"({self.left.pretty()} {self.operator} {self.right.pretty()})"

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        dot.node(node_id, f"LogicalExpr: {self.operator}")
        if parent_id:
            dot.edge(parent_id, node_id)
        self.left.to_dot(dot, node_id)
        self.right.to_dot(dot, node_id)

@dataclass
class TableRef(ASTNode):
    table_name: str
    alias: Optional[str] = None

    def pretty(self, indent: int = 0) -> str:
        if self.alias:
            return f"{self.table_name} AS {self.alias}"
        return self.table_name

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        label = f"Table: {self.table_name}"
        if self.alias:
            label += f" (AS {self.alias})"
        dot.node(node_id, label)
        if parent_id:
            dot.edge(parent_id, node_id)

@dataclass
class JoinClause(ASTNode):
    table: TableRef
    join_type: str  # e.g., 'INNER JOIN', 'LEFT JOIN'
    condition: Optional[ASTNode] = None

    def pretty(self, indent: int = 0) -> str:
        res = f"{self.join_type} {self.table.pretty()}"
        if self.condition:
            res += f" ON {self.condition.pretty()}"
        return res

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        dot.node(node_id, self.join_type)
        if parent_id:
            dot.edge(parent_id, node_id)
        self.table.to_dot(dot, node_id)
        if self.condition:
            self.condition.to_dot(dot, node_id)

@dataclass
class SelectItem(ASTNode):
    expression: ASTNode
    alias: Optional[str] = None

    def pretty(self, indent: int = 0) -> str:
        res = self.expression.pretty()
        if self.alias:
            res += f" AS {self.alias}"
        return res

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        label = "SelectItem"
        if self.alias:
            label += f" (AS {self.alias})"
        dot.node(node_id, label)
        if parent_id:
            dot.edge(parent_id, node_id)
        self.expression.to_dot(dot, node_id)

@dataclass
class SelectStatement(ASTNode):
    select_items: List[SelectItem]
    from_table: TableRef
    joins: List[JoinClause] = field(default_factory=list)
    where: Optional[ASTNode] = None

    def pretty(self, indent: int = 0) -> str:
        lines = []
        prefix = "  " * indent
        
        # SELECT
        items = ", ".join(item.pretty() for item in self.select_items)
        lines.append(f"{prefix}SELECT {items}")
        
        # FROM
        lines.append(f"{prefix}FROM {self.from_table.pretty()}")
        
        # JOINS
        for join in self.joins:
            lines.append(f"{prefix}{join.pretty()}")
            
        # WHERE
        if self.where:
            lines.append(f"{prefix}WHERE {self.where.pretty()}")
            
        return "\n".join(lines)

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        dot.node(node_id, "SELECT Statement")
        if parent_id:
            dot.edge(parent_id, node_id)
        
        # Select Items
        for item in self.select_items:
            item.to_dot(dot, node_id)
            
        # From Table
        self.from_table.to_dot(dot, node_id)
        
        # Joins
        for join in self.joins:
            join.to_dot(dot, node_id)
            
        # Where
        if self.where:
            self.where.to_dot(dot, node_id)
