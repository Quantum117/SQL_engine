from parser.parser import SQLParser
from catalog.catalog import Catalog, TableSchema
from planner.planner import LogicalPlanner
from planner.optimizer import LogicalOptimizer
from planner.physical_planner import PhysicalPlanner
from planner.vectorized_planner import VectorizedPlanner
import os
import csv
import numpy as np

def setup():
    # Setup data
    with open('data/sales.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'category', 'amount'])
        writer.writerow([1, 'Electronics', 100])
        writer.writerow([2, 'Furniture', 200])
        writer.writerow([3, 'Electronics', 150])
        writer.writerow([4, 'Furniture', 50])
        writer.writerow([5, 'Electronics', 300])

    catalog = Catalog()
    catalog.register_table(TableSchema.from_lists("sales", ["id", "category", "amount"], ["INT", "STR", "INT"], file_path="data/sales.csv"))
    return catalog

def test_aggregation():
    catalog = setup()
    parser = SQLParser()
    l_planner = LogicalPlanner(catalog)
    p_planner = PhysicalPlanner(catalog)
    v_planner = VectorizedPlanner(catalog)

    sql = "SELECT category, SUM(amount), COUNT(amount), AVG(amount) FROM sales GROUP BY category;"
    
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
    for k, v in batch.items():
        print(f"{k}: {v}")

if __name__ == "__main__":
    test_aggregation()
