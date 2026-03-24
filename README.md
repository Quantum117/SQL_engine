# Mini SQL Engine - Step 1: SQL Parsing and AST Construction

This step implements the foundation of the SQL engine: parsing raw SQL strings into a structured Abstract Syntax Tree (AST).

## Architecture Overview

### 1. SQL Grammar (`parser/grammar.lark`)
The grammar is defined using Lark's EBNF syntax. It supports a subset of SQL:
- **DQL**: `SELECT` statements with column references and table aliases.
- **Sources**: `FROM` clause with table names and optional aliases.
- **Joins**: `INNER JOIN` and `LEFT JOIN` with `ON` conditions.
- **Filters**: `WHERE` clause with binary comparisons (`=`, `!=`, `>`, `<`, `>=`, `<=`) and logical operators (`AND`, `OR`).

### 2. AST Nodes (`ast_nodes/nodes.py`)
Clean, strongly-typed representations of SQL constructs using Python `dataclasses`.
- `SelectStatement`: The root node containing items, tables, joins, and where clause.
- `TableRef`: Represents a table and its optional alias.
- `JoinClause`: Represents a join type, target table, and condition.
- `Expression`: Nodes for literals, column references, binary expressions, and logical expressions.

### 3. AST Builder (`parser/ast_builder.py`)
A `Transformer` class that traverses the Lark parse tree and maps it to the `dataclasses` in `nodes.py`. This separates the raw syntax handling from the high-level semantic representation.

### 4. Parser Integration (`parser/parser.py`)
The `SQLParser` class encapsulates the Lark engine and the transformer, providing a simple `parse(sql_text)` method.

## How to Run

1. **Install dependencies**:
   ```bash
   pip install lark
   ```

2. **Run the demo**:
   ```bash
   python -m parser.parser
   ```
   This will parse a sample query and print its pretty-printed AST.

## Design Decisions
- **Modularity**: Grammar, AST nodes, and transformation logic are separated into distinct files.
- **Dataclasses**: Used for AST nodes to provide immutability (mostly) and built-in support for comparison and representation.
- **Precedence**: Logical `AND` has higher precedence than `OR` in the grammar definition, matching standard SQL behavior.
