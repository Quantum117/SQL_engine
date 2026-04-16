from parser.parser import SQLParser
from catalog.catalog import Catalog, TableSchema
from planner.planner import LogicalPlanner
from planner.optimizer import LogicalOptimizer, ConstantFoldingRule, PredicatePushdownRule, ColumnPruningRule
from planner.physical_planner import PhysicalPlanner
from planner.vectorized_planner import VectorizedPlanner
import os
import csv

def setup():
    # Setup data
    with open('data/t1.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'val'])
        writer.writerow([1, 10])
        writer.writerow([2, 20])
    
    with open('data/t2.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name'])
        writer.writerow([1, 'A'])
        writer.writerow([2, 'B'])

    catalog = Catalog()
    catalog.register_table(TableSchema.from_lists("t1", ["id", "val"], ["INT", "INT"], file_path="data/t1.csv"))
    catalog.register_table(TableSchema.from_lists("t2", ["id", "name"], ["INT", "STR"], file_path="data/t2.csv"))
    return catalog

def test():
    catalog = setup()
    parser = SQLParser()
    l_planner = LogicalPlanner(catalog)
    optimizer = LogicalOptimizer()
    optimizer.add_rule(ConstantFoldingRule())
    optimizer.add_rule(PredicatePushdownRule())
    optimizer.add_rule(ColumnPruningRule())
    p_planner = PhysicalPlanner(catalog)
    v_planner = VectorizedPlanner(catalog)

    sql = "SELECT t1.val, t2.name FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.val > 15;"
    
    print("--- 1. Parsing ---")
    ast = parser.parse(sql)
    print(ast.pretty())

    print("\n--- 2. Logical Planning ---")
    l_plan = l_planner.plan(ast)
    print(l_plan.pretty())

    print("\n--- 3. Optimization ---")
    opt_plan = optimizer.optimize(l_plan)
    print(opt_plan.pretty())

    print("\n--- 4. Volcano Execution ---")
    p_plan = p_planner.plan(opt_plan)
    results = list(p_plan)
    print(f"Results ({len(results)}):", results)

    print("\n--- 5. Vectorized Execution ---")
    v_plan = v_planner.plan(opt_plan)
    batch = v_plan.next_batch()
    print("Batch Results:", batch)

if __name__ == "__main__":
    test()
