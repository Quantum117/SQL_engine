from parser.parser import SQLParser
from catalog.catalog import Catalog, TableSchema
from planner.planner import LogicalPlanner
from planner.physical_planner import PhysicalPlanner
from planner.vectorized_planner import VectorizedPlanner
from planner.optimizer import LogicalOptimizer
import os
import csv
import time

def setup():
    # Setup data
    with open('data/indexing_data.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'email', 'name'])
        for i in range(1, 101):
            writer.writerow([i, f'user{i}@example.com', f'User {i}'])

    catalog = Catalog()
    schema = TableSchema.from_lists("indexing_data", ["id", "email", "name"], ["INT", "STR", "STR"], file_path="data/indexing_data.csv")
    schema.add_index("email", "HASH")
    catalog.register_table(schema)
    return catalog

def test_indexing():
    catalog = setup()
    parser = SQLParser()
    l_planner = LogicalPlanner(catalog)
    optimizer = LogicalOptimizer(catalog)
    p_planner = PhysicalPlanner(catalog)
    v_planner = VectorizedPlanner(catalog)

    # Point lookup on indexed column
    sql = "SELECT name FROM indexing_data WHERE email = 'user42@example.com';"
    
    print("--- 1. Parsing ---")
    ast = parser.parse(sql)
    print(ast.pretty())

    print("\n--- 2. Logical Planning & Optimization ---")
    l_plan = l_planner.plan(ast)
    print("Naive Plan:")
    print(l_plan.pretty())
    
    opt_plan = optimizer.optimize(l_plan)
    print("\nOptimized Plan (IndexRule applied):")
    print(opt_plan.pretty())

    if "LogicalIndexScan" not in opt_plan.pretty():
        print("FAIL: IndexRule did not apply!")
        return

    print("\n--- 3. Volcano Execution (IndexScan) ---")
    p_plan = p_planner.plan(opt_plan)
    results = list(p_plan)
    for r in results:
        print(f"Result: {r}")
    print(f"Rows processed: {p_plan.input.get_stats()['processed_rows']} (Expected 1 if index worked)")

    print("\n--- 4. Vectorized Execution (IndexScan) ---")
    v_plan = v_planner.plan(opt_plan)
    batch = v_plan.next_batch()
    print("Batch Results:")
    if batch:
        for k, v in batch.items():
            print(f"{k}: {v}")
    print(f"Rows processed: {v_plan.input.get_stats()['processed_rows']} (Expected 1 if index worked)")

if __name__ == "__main__":
    test_indexing()
