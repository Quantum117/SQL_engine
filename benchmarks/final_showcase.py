import time
import os
import csv
import numpy as np
from parser.parser import SQLParser
from catalog.catalog import Catalog, TableSchema
from planner.planner import LogicalPlanner
from planner.optimizer import LogicalOptimizer, ConstantFoldingRule, PredicatePushdownRule, ColumnPruningRule
from planner.physical_planner import PhysicalPlanner
from planner.vectorized_planner import VectorizedPlanner

def draw_bar(label, value, max_val, color_code):
    """Draws a clean ASCII bar chart for the terminal."""
    width = 30
    bar_len = int((value / max_val) * width) if max_val > 0 else 0
    bar = "#" * bar_len
    # ANSI coloring: Red (31), Yellow (33), Green (32)
    print(f"{label:12} | \033[{color_code}m{bar:<{width}}\033[0m | {value:.2f} ms")

def generate_simple_chart(results):
    """Generates a professional 3-bar comparison chart."""
    try:
        import matplotlib.pyplot as plt
        import matplotlib
        matplotlib.use('Agg')
        
        labels = [r[0] for r in results]
        times = [r[1] for r in results]
        colors = ['#ff7675', '#fdcb6e', '#55efc4']
        
        plt.figure(figsize=(9, 6))
        bars = plt.bar(labels, times, color=colors, edgecolor='black', alpha=0.9)
        
        plt.ylabel('Execution Time (ms)', fontsize=12, fontweight='bold')
        plt.title('SQL Engine Performance: 10,000 Users Benchmark', fontsize=14, fontweight='bold', pad=20)
        plt.grid(axis='y', linestyle='--', alpha=0.3)
        
        # Add values on top of bars
        for bar in bars:
            yval = bar.get_height()
            plt.text(bar.get_x() + bar.get_width()/2, yval + (max(times)*0.01), 
                     f'{yval:.2f} ms', ha='center', va='bottom', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig('reports/benchmark_chart.png', dpi=300)
    except Exception as e:
        print(f"\n[!] Matplotlib error: {e}")

def run_showcase():
    path_data = "/benchmarks/data\\"
    # Load Benchmark Data
    catalog = Catalog()
    for table, file in [("users", path_data+"showcase_users.csv"), ("products", path_data+"showcase_products.csv"), ("orders", path_data+"showcase_orders.csv")]:
        if not os.path.exists(file):
            print(f"[!] Missing {file}. Please ensure data is generated.")
            return

    catalog.register_table(TableSchema.from_lists("users", ["id", "name", "age"], ["INT", "STR", "INT"], file_path=path_data+"showcase_users.csv"))
    catalog.register_table(TableSchema.from_lists("products", ["id", "name", "category", "status"], ["INT", "STR", "STR", "STR"], file_path=path_data+"showcase_products.csv"))
    catalog.register_table(TableSchema.from_lists("orders", ["id", "user_id", "product_id", "amount"], ["INT", "INT", "INT", "FLOAT"], file_path=path_data+"showcase_orders.csv"))
    
    parser = SQLParser()
    l_planner = LogicalPlanner(catalog)
    p_planner = PhysicalPlanner(catalog)
    v_planner = VectorizedPlanner(catalog)
    
    # Standard Optimizer
    optimizer = LogicalOptimizer(catalog)
    optimizer.add_rule(PredicatePushdownRule())
    optimizer.add_rule(ColumnPruningRule())

    # The Query
    sql = "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON o.product_id = p.id WHERE u.age > 40 AND p.status = 'inactive'"
    ast = parser.parse(sql)
    
    print("\n" + "="*50)
    print("SQL ENGINE PERFORMANCE BENCHMARK")
    print("="*50)

    # 1. VANILLA
    print(f"[1/3] Running Vanilla (Original)...", end="", flush=True)
    plan_v = p_planner.plan(l_planner.plan(ast))
    start = time.perf_counter()
    list(plan_v)
    t_naive = (time.perf_counter() - start) * 1000
    print(f" DONE ({t_naive:.2f} ms)")

    # 2. OPTIMIZED
    print(f"[2/3] Running Optimized (RBO)...     ", end="", flush=True)
    plan_opt = p_planner.plan(optimizer.optimize(l_planner.plan(ast)))
    start = time.perf_counter()
    list(plan_opt)
    t_opt = (time.perf_counter() - start) * 1000
    print(f" DONE ({t_opt:.2f} ms)")

    # 3. VECTORIZED
    print(f"[3/3] Running Vectorized (NumPy)...   ", end="", flush=True)
    v_plan = v_planner.plan(optimizer.optimize(l_planner.plan(ast)))
    start = time.perf_counter()
    while v_plan.next_batch(1024) is not None: pass
    t_v = (time.perf_counter() - start) * 1000
    print(f" DONE ({t_v:.2f} ms)")

    print("\n" + "-"*50)
    print("COMPARISON (Lower is Better)")
    print("-"*50)
    max_t = max(t_naive, t_opt, t_v)
    draw_bar("Vanilla", t_naive, max_t, "31")
    draw_bar("Optimized", t_opt, max_t, "33")
    draw_bar("Vectorized", t_v, max_t, "32")
    
    generate_simple_chart([("Vanilla", t_naive), ("Optimized", t_opt), ("Vectorized", t_v)])
    
    print("\n[OK] Final comparison chart saved as 'reports/benchmark_chart.png'")
    print(f"VERDICT: Vectorized is {t_naive/max(t_v,0.1):.1f}x faster than Vanilla.")
    print("="*50 + "\n")

if __name__ == "__main__":
    run_showcase()
