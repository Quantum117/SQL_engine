from parser.parser import SQLParser
from catalog.catalog import Catalog, TableSchema
from planner.planner import LogicalPlanner
from planner.physical_planner import PhysicalPlanner
from planner.vectorized_planner import VectorizedPlanner
import os
import csv

def setup():
    # Setup data
    with open('data/users.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'age'])
        writer.writerow([1, 'Alice', 25])
        writer.writerow([2, 'Bob', 30])
        writer.writerow([3, 'Charlie', 35])

    catalog = Catalog()
    catalog.register_table(TableSchema.from_lists("users", ["id", "name", "age"], ["INT", "STR", "INT"], file_path="data/users.csv"))
    return catalog

def test_subquery():
    catalog = setup()
    parser = SQLParser()
    l_planner = LogicalPlanner(catalog)
    p_planner = PhysicalPlanner(catalog)
    v_planner = VectorizedPlanner(catalog)

    # Simple subquery: Get names of users older than 28 from a filtered subquery
    sql = "SELECT name FROM (SELECT name, age FROM users WHERE age > 28) t WHERE age < 35;"
    
    print("--- 1. Parsing ---")
    ast = parser.parse(sql)
    print(ast.pretty())

    print("\n--- 2. Logical Planning ---")
    l_plan = l_planner.plan(ast)
    print(l_plan.pretty())

    print("\n--- 3. Volcano Execution ---")
    p_plan = p_planner.plan(l_plan)
    results = list(p_plan)
    for r in results:
        print(r)

    print("\n--- 4. Vectorized Execution ---")
    v_plan = v_planner.plan(l_plan)
    batch = v_plan.next_batch()
    print("Batch Results:")
    if batch:
        for k, v in batch.items():
            print(f"{k}: {v}")

if __name__ == "__main__":
    test_subquery()
