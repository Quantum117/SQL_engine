import os
from parser.parser import SQLParser
from catalog.catalog import Catalog, TableSchema
from planner.planner import LogicalPlanner
from planner.optimizer import LogicalOptimizer, PredicatePushdownRule, ColumnPruningRule, ConstantFoldingRule
from visualization.visualizer import LogicalPlanVisualizer, MermaidVisualizer, ConsoleVisualizer

def run_optimization_visualization():
    # ... (Setup Catalog and Tools) ...
    catalog = Catalog()
    catalog.register_table(TableSchema.from_lists(
        "users", 
        ["id", "name", "age", "status"], 
        ["INT", "STR", "INT", "STR"]
    ))
    catalog.register_table(TableSchema.from_lists(
        "orders", 
        ["id", "user_id", "amount", "date"], 
        ["INT", "INT", "FLOAT", "STR"]
    ))

    parser = SQLParser()
    planner = LogicalPlanner(catalog)
    optimizer = LogicalOptimizer(catalog)
    optimizer.add_rule(ConstantFoldingRule())
    optimizer.add_rule(PredicatePushdownRule())
    optimizer.add_rule(ColumnPruningRule())
    
    png_vis = LogicalPlanVisualizer(output_dir="reports")
    con_vis = ConsoleVisualizer()

    # 3. The Query
    sql = """
    SELECT u.name, o.amount 
    FROM users u 
    JOIN orders o ON u.id = o.user_id 
    WHERE u.age > 25 AND o.amount > 100 AND (10 + 20) > 20
    """

    print(f"Parsing Query: {sql.strip()}")
    ast = parser.parse(sql)
    
    # 4. Initial Plan
    print("\n[1/2] Generating Initial Logical Plan...")
    initial_plan = planner.plan(ast)
    con_vis.visualize(initial_plan, "Initial Logical Plan")
    png_vis.visualize(initial_plan, "plan_1_initial")
    
    # 5. Optimized Plan
    print("\n[2/2] Running Optimizer (RBO)...")
    optimized_plan = optimizer.optimize(initial_plan)
    con_vis.visualize(optimized_plan, "Optimized Logical Plan")
    png_vis.visualize(optimized_plan, "plan_2_optimized")
    
    print("\n" + "="*60)
    print("SUCCESS: Professional visualizations saved to 'reports/'")
    print("Check the terminal above for the instant structural view.")
    print("="*60)

if __name__ == "__main__":
    run_optimization_visualization()
