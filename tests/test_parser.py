import pytest
from parser.parser import SQLParser

@pytest.fixture
def parser():
    return SQLParser()

def test_simple_select(parser):
    sql = "SELECT name FROM employees WHERE age > 30;"
    ast = parser.parse(sql)
    assert ast.from_table.table_name == "employees"
    assert len(ast.select_items) == 1
    assert ast.select_items[0].expression.name == "name"
    assert ast.where.operator == ">"

def test_select_alias_and_join(parser):
    sql = """
    SELECT e.name, d.department_name
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    WHERE e.age > 30;
    """
    ast = parser.parse(sql)
    assert ast.from_table.table_name == "employees"
    assert ast.from_table.alias == "e"
    assert len(ast.joins) == 1
    assert ast.joins[0].table.table_name == "departments"
    assert ast.joins[0].table.alias == "d"
    assert ast.joins[0].join_type == "INNER JOIN"

def test_left_join(parser):
    sql = """
    SELECT e.name, d.department_name
    FROM employees e
    LEFT JOIN departments d ON e.department_id = d.id
    WHERE e.age > 30;
    """
    ast = parser.parse(sql)
    assert ast.joins[0].join_type == "LEFT JOIN"

def test_logical_operators(parser):
    sql = "SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3;"
    # Note: Grammar doesn't support SELECT * yet, let's use a specific column
    sql = "SELECT id FROM t WHERE a = 1 AND b = 2 OR c = 3;"
    ast = parser.parse(sql)
    # Check tree structure (AND higher precedence than OR is typical, but let's check what our grammar does)
    # ?logical_or: logical_and ("OR" logical_and)*
    # ?logical_and: comparison ("AND" comparison)*
    # So it should be (a=1 AND b=2) OR (c=3)
    assert ast.where.operator == "OR"
    assert ast.where.left.operator == "AND"

def test_demo_examples(parser):
    # Example 1
    sql1 = "SELECT name FROM employees WHERE age > 30;"
    ast1 = parser.parse(sql1)
    print(ast1.pretty())
    
    # Example 2
    sql2 = """
    SELECT e.name, d.department_name
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    WHERE e.age > 30;
    """
    ast2 = parser.parse(sql2)
    print(ast2.pretty())
    
    # Example 3
    sql3 = """
    SELECT e.name, d.department_name
    FROM employees e
    LEFT JOIN departments d ON e.department_id = d.id
    WHERE e.age > 30;
    """
    ast3 = parser.parse(sql3)
    print(ast3.pretty())
