import time
import os
import csv
from parser.parser import SQLParser
from catalog.catalog import Catalog, TableSchema
from planner.planner import LogicalPlanner
from planner.optimizer import LogicalOptimizer, ConstantFoldingRule, PredicatePushdownRule, ColumnPruningRule
from planner.physical_planner import PhysicalPlanner
from planner.vectorized_planner import VectorizedPlanner


def setup_data():
    """Generates a small dataset for fast verification."""
    # 10 Users
    with open("verify_users.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "age"])
        for i in range(10):
            writer.writerow([i, f"User_{i}", 20 + i * 5])

    # 10 Products
    with open("verify_products.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "category", "status"])
        for i in range(10):
            status = "active" if i % 2 == 0 else "inactive"
            writer.writerow([i, f"Prod_{i}", "Test", status])

    # 50 Orders
    with open("verify_orders.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "user_id", "product_id", "amount"])
        for i in range(50):
            writer.writerow([i, i % 10, i % 10, 100 + i])


def run_verification():
    setup_data()
    catalog = Catalog()
    catalog.register_table(
        TableSchema.from_lists("users", ["id", "name", "age"], ["INT", "STR", "INT"], file_path="verify_users.csv"))
    catalog.register_table(
        TableSchema.from_lists("products", ["id", "name", "category", "status"], ["INT", "STR", "STR", "STR"],
                               file_path="verify_products.csv"))
    catalog.register_table(
        TableSchema.from_lists("orders", ["id", "user_id", "product_id", "amount"], ["INT", "INT", "INT", "FLOAT"],
                               file_path="verify_orders.csv"))

    parser = SQLParser()
    l_planner = LogicalPlanner(catalog)
    p_planner = PhysicalPlanner(catalog)
    v_planner = VectorizedPlanner(catalog)

    sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE u.age > 40"
    ast = parser.parse(sql)

    print(f"--- Verifying Results for: {sql} ---")

    # 1. Naive
    l_plan = l_planner.plan(ast)
    p_plan = p_planner.plan(l_plan)
    start = time.perf_counter()
    res1 = list(p_plan)
    t_naive = (time.perf_counter() - start) * 1000
    print(f"Naive Time: {t_naive:.2f} ms")

    # 2. Optimized (Pushdown + Pruning)
    optimizer = LogicalOptimizer(catalog)
    optimizer.add_rule(PredicatePushdownRule())
    optimizer.add_rule(ColumnPruningRule())
    opt_l_plan = optimizer.optimize(l_planner.plan(ast))
    opt_p_plan = p_planner.plan(opt_l_plan)
    start = time.perf_counter()
    res2 = list(opt_p_plan)
    t_opt = (time.perf_counter() - start) * 1000
    print(f"Optimized Time: {t_opt:.2f} ms (Speedup: {t_naive / max(t_opt, 0.001):.2f}x)")


if __name__ == "__main__":
    run_verification()
