import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from parser.parser import SQLParser
from planner.planner import LogicalPlanner
from planner.physical_planner import PhysicalPlanner
from catalog.catalog import Catalog, TableSchema
from visualization.visualizer import PhysicalPlanVisualizer

def run_demo():
    # 1. Setup Catalog and Data
    catalog = Catalog()
    # Define sales table schema
    sales_schema = TableSchema("sales", ["id", "category", "amount"], ["INT", "STR", "INT"])
    sales_schema.file_path = os.path.abspath("benchmarks/data/sales.csv")
    catalog.register_table(sales_schema)

    # 2. Query
    sql = "SELECT category, SUM(amount) FROM sales WHERE amount > 100 GROUP BY category;"
    print(f"Query: {sql}")

    # 3. Parse and Plan
    parser = SQLParser()
    logical_planner = LogicalPlanner(catalog)
    physical_planner = PhysicalPlanner(catalog)

    ast = parser.parse(sql)
    logical_plan = logical_planner.plan(ast)
    physical_plan = physical_planner.plan(logical_plan)

    # 4. Execute to populate stats
    print("Executing query...")
    results = []
    try:
        for row in physical_plan:
            results.append(row)
    except StopIteration:
        pass
    
    print(f"Results: {results}")

    # 5. Visualize
    print("Generating Physical Execution DAG...")
    viz = PhysicalPlanVisualizer(output_dir="output")
    output_png = viz.visualize(physical_plan, "demo_physical_dag")
    print(f"Success! Visualization saved to: {output_png}")

if __name__ == "__main__":
    run_demo()
