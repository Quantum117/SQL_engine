from typing import List, Optional, Any
from dataclasses import dataclass, field
import html

@dataclass
class LogicalOperator:
    def pretty(self, indent: int = 0) -> str:
        raise NotImplementedError

    def to_terminal(self, indent: int = 0, is_last: bool = True, prefix: str = "") -> str:
        """Generates a clean ASCII tree for the terminal."""
        raise NotImplementedError

    def to_dot(self, dot, parent_id: Optional[str] = None):
        raise NotImplementedError

    def to_mermaid(self, lines: List[str], parent_id: Optional[str] = None):
        raise NotImplementedError

@dataclass
class LogicalScan(LogicalOperator):
    table_name: str
    columns: List[str] # Columns to be read
    alias: Optional[str] = None

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        return f"{prefix}LogicalScan(table={self.table_name}, columns={self.columns}, alias={self.alias})"

    def to_terminal(self, indent: int = 0, is_last: bool = True, prefix: str = "") -> str:
        marker = "+-- " if is_last else "|-- "
        alias_str = f" AS {self.alias}" if self.alias else ""
        return f"{prefix}{marker}SCAN: {self.table_name}{alias_str} (cols: {self.columns})\n"

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        alias_info = f" AS {self.alias}" if self.alias else ""
        label = f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4"><TR><TD BGCOLOR="lightblue"><B>SCAN: {self.table_name}{alias_info}</B></TD></TR><TR><TD ALIGN="LEFT">Cols: {html.escape(str(self.columns))}</TD></TR></TABLE>>'
        dot.node(node_id, label, shape="none")
        if parent_id: dot.edge(parent_id, node_id)

    def to_mermaid(self, lines: List[str], parent_id: Optional[str] = None):
        node_id = f"scan_{id(self) % 10000}"
        label = f"SCAN: {self.table_name}"
        lines.append(f'  {node_id}["{label}"]')
        if parent_id: lines.append(f"  {parent_id} --> {node_id}")

@dataclass
class LogicalFilter(LogicalOperator):
    input: LogicalOperator
    condition: Any # Unified expression node

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        res = f"{prefix}LogicalFilter(cond={self.condition})\n"
        res += self.input.pretty(indent + 1)
        return res

    def to_terminal(self, indent: int = 0, is_last: bool = True, prefix: str = "") -> str:
        marker = "+-- " if is_last else "|-- "
        res = f"{prefix}{marker}FILTER: {self.condition.pretty()}\n"
        new_prefix = prefix + ("    " if is_last else "|   ")
        res += self.input.to_terminal(indent + 1, True, new_prefix)
        return res

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        cond_str = html.escape(self.condition.pretty())
        label = f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4"><TR><TD BGCOLOR="lightyellow"><B>FILTER</B></TD></TR><TR><TD ALIGN="LEFT">{cond_str}</TD></TR></TABLE>>'
        dot.node(node_id, label, shape="none")
        if parent_id: dot.edge(parent_id, node_id)
        self.input.to_dot(dot, node_id)

    def to_mermaid(self, lines: List[str], parent_id: Optional[str] = None):
        node_id = f"filter_{id(self) % 10000}"
        cond_str = self.condition.pretty() if hasattr(self.condition, 'pretty') else str(self.condition)
        label = f"FILTER: {cond_str}"
        lines.append(f'  {node_id}{{"{label}"}}')
        if parent_id: lines.append(f"  {parent_id} --> {node_id}")
        self.input.to_mermaid(lines, node_id)

@dataclass
class LogicalProject(LogicalOperator):
    input: LogicalOperator
    expressions: List[Any] # Column references or expressions
    aliases: List[Optional[str]]

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        items = [f"{expr}" + (f" AS {alias}" if alias else "") for expr, alias in zip(self.expressions, self.aliases)]
        res = f"{prefix}LogicalProject(items=[{', '.join(items)}])\n"
        res += self.input.pretty(indent + 1)
        return res

    def to_terminal(self, indent: int = 0, is_last: bool = True, prefix: str = "") -> str:
        marker = "+-- " if is_last else "|-- "
        items = [f"{expr.pretty()}" + (f" AS {alias}" if alias else "") for expr, alias in zip(self.expressions, self.aliases)]
        res = f"{prefix}{marker}PROJECT: {', '.join(items)}\n"
        new_prefix = prefix + ("    " if is_last else "|   ")
        res += self.input.to_terminal(indent + 1, True, new_prefix)
        return res

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        items = [f"{expr.pretty()}" + (f" AS {alias}" if alias else "") for expr, alias in zip(self.expressions, self.aliases)]
        items_str = html.escape(", ".join(items))
        label = f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4"><TR><TD BGCOLOR="lightgrey"><B>PROJECT</B></TD></TR><TR><TD ALIGN="LEFT">{items_str}</TD></TR></TABLE>>'
        dot.node(node_id, label, shape="none")
        if parent_id: dot.edge(parent_id, node_id)
        self.input.to_dot(dot, node_id)

    def to_mermaid(self, lines: List[str], parent_id: Optional[str] = None):
        node_id = f"proj_{id(self) % 10000}"
        items = [f"{expr.pretty() if hasattr(expr, 'pretty') else expr}" + (f" AS {alias}" if alias else "") 
                 for expr, alias in zip(self.expressions, self.aliases)]
        label = f"PROJECT: {', '.join(items)}"
        lines.append(f'  {node_id}["{label}"]')
        if parent_id: lines.append(f"  {parent_id} --> {node_id}")
        self.input.to_mermaid(lines, node_id)

@dataclass
class LogicalJoin(LogicalOperator):
    left: LogicalOperator
    right: LogicalOperator
    join_type: str
    condition: Optional[Any]

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        res = f"{prefix}LogicalJoin(type={self.join_type}, cond={self.condition})\n"
        res += self.left.pretty(indent + 1) + "\n"
        res += self.right.pretty(indent + 1)
        return res

    def to_terminal(self, indent: int = 0, is_last: bool = True, prefix: str = "") -> str:
        marker = "+-- " if is_last else "|-- "
        cond_str = self.condition.pretty() if self.condition else "True"
        res = f"{prefix}{marker}JOIN ({self.join_type}): {cond_str}\n"
        new_prefix = prefix + ("    " if is_last else "|   ")
        res += self.left.to_terminal(indent + 1, False, new_prefix)
        res += self.right.to_terminal(indent + 1, True, new_prefix)
        return res

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        cond_str = html.escape(self.condition.pretty() if self.condition else "True")
        label = f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4"><TR><TD BGCOLOR="lightcoral"><B>JOIN ({self.join_type})</B></TD></TR><TR><TD ALIGN="LEFT">{cond_str}</TD></TR></TABLE>>'
        dot.node(node_id, label, shape="none")
        if parent_id: dot.edge(parent_id, node_id)
        self.left.to_dot(dot, node_id)
        self.right.to_dot(dot, node_id)

    def to_mermaid(self, lines: List[str], parent_id: Optional[str] = None):
        node_id = f"join_{id(self) % 10000}"
        cond_str = self.condition.pretty() if self.condition and hasattr(self.condition, 'pretty') else str(self.condition)
        label = f"JOIN ({self.join_type}): {cond_str}"
        lines.append(f'  {node_id}(["{label}"])')
        if parent_id: lines.append(f"  {parent_id} --> {node_id}")
        self.left.to_mermaid(lines, node_id)
        self.right.to_mermaid(lines, node_id)

@dataclass
class LogicalAggregate(LogicalOperator):
    input: LogicalOperator
    group_by: List[str] # Column names
    aggregates: List[Any] # AggregateExpression nodes

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        aggs = ", ".join(str(a) for a in self.aggregates)
        groups = ", ".join(self.group_by)
        res = f"{prefix}LogicalAggregate(groups=[{groups}], aggs=[{aggs}])\n"
        res += self.input.pretty(indent + 1)
        return res

    def to_terminal(self, indent: int = 0, is_last: bool = True, prefix: str = "") -> str:
        marker = "+-- " if is_last else "|-- "
        aggs = ", ".join(a.pretty() for a in self.aggregates)
        groups = ", ".join(self.group_by)
        res = f"{prefix}{marker}AGGREGATE: {aggs}{' BY ' + groups if groups else ''}\n"
        new_prefix = prefix + ("    " if is_last else "|   ")
        res += self.input.to_terminal(indent + 1, True, new_prefix)
        return res

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        aggs = html.escape(", ".join(a.pretty() for a in self.aggregates))
        groups = html.escape(", ".join(self.group_by))
        label = f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4"><TR><TD BGCOLOR="lightgreen"><B>AGGREGATE</B></TD></TR><TR><TD ALIGN="LEFT">{aggs}</TD></TR><TR><TD ALIGN="LEFT">GROUP BY: {groups}</TD></TR></TABLE>>'
        dot.node(node_id, label, shape="none")
        if parent_id: dot.edge(parent_id, node_id)
        self.input.to_dot(dot, node_id)

    def to_mermaid(self, lines: List[str], parent_id: Optional[str] = None):
        node_id = f"agg_{id(self) % 10000}"
        aggs = ", ".join(a.pretty() if hasattr(a, 'pretty') else str(a) for a in self.aggregates)
        groups = ", ".join(self.group_by)
        label = f"AGGREGATE: {aggs}<br/>BY: {', '.join(groups)}"
        lines.append(f'  {node_id}["{label}"]')
        if parent_id: lines.append(f"  {parent_id} --> {node_id}")
        self.input.to_mermaid(lines, node_id)

@dataclass
class LogicalIndexScan(LogicalOperator):
    table_name: str
    index_column: str
    value: Any
    columns: List[str]
    alias: Optional[str] = None

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        return f"{prefix}LogicalIndexScan(table={self.table_name}, on={self.index_column}, val={self.value}, columns={self.columns}, alias={self.alias})"

    def to_terminal(self, indent: int = 0, is_last: bool = True, prefix: str = "") -> str:
        marker = "+-- " if is_last else "|-- "
        alias_str = f" AS {self.alias}" if self.alias else ""
        return f"{prefix}{marker}INDEX SCAN: {self.table_name}{alias_str} (on {self.index_column}={self.value})\n"

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        alias_info = f" AS {self.alias}" if self.alias else ""
        label = f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4"><TR><TD BGCOLOR="cyan"><B>INDEX SCAN: {self.table_name}{alias_info}</B></TD></TR><TR><TD ALIGN="LEFT">ON: {html.escape(self.index_column)}={html.escape(str(self.value))}</TD></TR></TABLE>>'
        dot.node(node_id, label, shape="none")
        if parent_id: dot.edge(parent_id, node_id)

    def to_mermaid(self, lines: List[str], parent_id: Optional[str] = None):
        node_id = f"iscan_{id(self) % 10000}"
        label = f"INDEX SCAN: {self.table_name}<br/>on {self.index_column}={self.value}"
        lines.append(f'  {node_id}["{label}"]')
        if parent_id: lines.append(f"  {parent_id} --> {node_id}")

@dataclass
class LogicalLimit(LogicalOperator):
    input: LogicalOperator
    limit: int

    def pretty(self, indent: int = 0) -> str:
        prefix = "  " * indent
        res = f"{prefix}LogicalLimit(limit={self.limit})\n"
        res += self.input.pretty(indent + 1)
        return res

    def to_terminal(self, indent: int = 0, is_last: bool = True, prefix: str = "") -> str:
        marker = "+-- " if is_last else "|-- "
        res = f"{prefix}{marker}LIMIT: {self.limit}\n"
        new_prefix = prefix + ("    " if is_last else "|   ")
        res += self.input.to_terminal(indent + 1, True, new_prefix)
        return res

    def to_dot(self, dot, parent_id: Optional[str] = None):
        node_id = str(id(self))
        label = f'<<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4"><TR><TD BGCOLOR="plum"><B>LIMIT: {self.limit}</B></TD></TR></TABLE>>'
        dot.node(node_id, label, shape="none")
        if parent_id: dot.edge(parent_id, node_id)
        self.input.to_dot(dot, node_id)

    def to_mermaid(self, lines: List[str], parent_id: Optional[str] = None):
        node_id = f"limit_{id(self) % 10000}"
        label = f"LIMIT: {self.limit}"
        lines.append(f'  {node_id}["{label}"]')
        if parent_id: lines.append(f"  {parent_id} --> {node_id}")
        self.input.to_mermaid(lines, node_id)
