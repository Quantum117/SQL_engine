from parser.parser import SQLParser
from catalog.catalog import Catalog, TableSchema
from planner.planner import LogicalPlanner
from planner.optimizer import LogicalOptimizer, PredicatePushdownRule
import os

def check():
    catalog = Catalog()
    catalog.register_table(TableSchema.from_lists("users", ["id", "name", "age"], ["INT", "STR", "INT"], file_path="users.csv"))
    catalog.register_table(TableSchema.from_lists("orders", ["id", "user_id", "product_id", "amount"], ["INT", "INT", "INT", "FLOAT"], file_path="orders.csv"))
    catalog.register_table(TableSchema.from_lists("products", ["id", "name", "category", "status"], ["INT", "STR", "STR", "STR"], file_path="products.csv"))
    
    parser = SQLParser()
    l_planner = LogicalPlanner(catalog)
    optimizer = LogicalOptimizer(catalog)
    optimizer.add_rule(PredicatePushdownRule())
    
    sql = """
    SELECT u.name, o.amount, p.category 
    FROM users u 
    JOIN orders o ON u.id = o.user_id 
    JOIN products p ON o.product_id = p.id 
    WHERE u.age > 75 AND o.amount > 450 AND p.status = 'inactive';
    """
    
    ast = parser.parse(sql)
    plan = l_planner.plan(ast)
    print("--- Original Plan ---")
    print(plan.pretty())
    
    opt_plan = optimizer.optimize(plan)
    print("\n--- Optimized Plan ---")
    print(opt_plan.pretty())

if __name__ == "__main__":
    check()
