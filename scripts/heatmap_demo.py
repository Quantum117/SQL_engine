import sys
import os
import time

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from parser.parser import SQLParser
from planner.planner import LogicalPlanner
from planner.physical_planner import PhysicalPlanner
from planner.optimizer import LogicalOptimizer, PredicatePushdownRule
from catalog.catalog import Catalog, TableSchema
from visualization.visualizer import HeatmapVisualizer

def run_demo():
    # 1. Setup Catalog
    catalog = Catalog()
    path_data = "data/"
    
    catalog.register_table(TableSchema.from_lists("users", ["id", "name", "age"], ["INT", "STR", "INT"], 
                                                file_path=os.path.abspath(path_data + "showcase_users.csv")))
    catalog.register_table(TableSchema.from_lists("orders", ["id", "user_id", "product_id", "amount"], ["INT", "INT", "INT", "FLOAT"], 
                                                file_path=os.path.abspath(path_data + "showcase_orders.csv")))

    # 2. Query (Compute-heavy JOIN and Filter)
    sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 30 AND o.amount > 50;"
    print(f"Query: {sql}")

    # 3. Plan
    parser = SQLParser()
    logical_planner = LogicalPlanner(catalog)
    optimizer = LogicalOptimizer(catalog)
    optimizer.add_rule(PredicatePushdownRule())
    physical_planner = PhysicalPlanner(catalog)

    l_plan = logical_planner.plan(parser.parse(sql))
    l_plan_opt = optimizer.optimize(l_plan)
    p_plan = physical_planner.plan(l_plan_opt)

    # 4. Execute to gather timing stats
    print("Executing query and profiling operators...")
    results = []
    try:
        for row in p_plan:
            results.append(row)
    except StopIteration:
        pass
    
    print(f"Done! Processed {len(results)} results.")

    # 5. Visualize Heatmap
    print("Generating Operator Performance Heatmap...")
    viz = HeatmapVisualizer(output_dir="output")
    output_png = viz.visualize(p_plan, "performance_heatmap")
    print(f"Success! Heatmap saved to: {output_png}")

if __name__ == "__main__":
    run_demo()
