import os
import sys
import time

# Add root folder to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from parser.parser import SQLParser
from catalog.catalog import Catalog, TableSchema
from planner.planner import LogicalPlanner
from planner.optimizer import LogicalOptimizer, PredicatePushdownRule, ColumnPruningRule
from planner.physical_planner import PhysicalPlanner
from planner.vectorized_planner import VectorizedPlanner

def format_table(rows, columns):
    """Simple formatting function to print lists of dicts as a pretty terminal table"""
    if not rows:
        return "Empty result set."
    
    # Calculate column widths
    col_widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            col_widths[col] = max(col_widths[col], len(str(row.get(col, ""))))
            
    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    separator = "-+-".join("-" * col_widths[col] for col in columns)
    
    out = [header, separator]
    for row in rows:
        r = " | ".join(str(row.get(col, "")).ljust(col_widths[col]) for col in columns)
        out.append(r)
    return "\n".join(out)

def setup_engine():
    # Load Benchmark Data
    catalog = Catalog()
    path_data = os.path.join("benchmarks", "data")
    
    try:
        catalog.register_table(TableSchema.from_lists("users", ["id", "name", "age"], ["INT", "STR", "INT"], file_path=os.path.join(path_data, "showcase_users.csv")))
        catalog.register_table(TableSchema.from_lists("products", ["id", "name", "category", "status"], ["INT", "STR", "STR", "STR"], file_path=os.path.join(path_data, "showcase_products.csv")))
        catalog.register_table(TableSchema.from_lists("orders", ["id", "user_id", "product_id", "amount"], ["INT", "INT", "INT", "FLOAT"], file_path=os.path.join(path_data, "showcase_orders.csv")))
    except Exception as e:
        print(f"Failed to load sample tables: {e}")
        
    parser = SQLParser()
    l_planner = LogicalPlanner(catalog)
    p_planner = PhysicalPlanner(catalog)
    
    optimizer = LogicalOptimizer(catalog)
    optimizer.add_rule(PredicatePushdownRule())
    optimizer.add_rule(ColumnPruningRule())
    
    return catalog, parser, l_planner, optimizer, p_planner

def main():
    print("="*60)
    print("🚀 Mini SQL Engine REPL 🚀")
    print("Type your SQL query and press Enter. (Type 'exit' to quit)")
    print("Available tables: users (id, name, age), products (...), orders (...)")
    print("="*60)

    catalog, parser, l_planner, optimizer, p_planner = setup_engine()

    while True:
        try:
            sql = input("\033[94mSQL> \033[0m").strip()
            if sql.lower() in ('exit', 'quit', '\\q'):
                print("Goodbye!")
                break
            if not sql:
                continue
            if not sql.endswith(';'):
                sql += ';'

            t_start = time.perf_counter()
            
            # 1. Parse
            ast = parser.parse(sql)
            # 2. Plan
            l_plan = l_planner.plan(ast)
            # 3. Optimize
            o_plan = optimizer.optimize(l_plan)
            # 4. Physical Exec (Volcano chosen here for ease of converting to dicts directly)
            p_plan = p_planner.plan(o_plan)
            
            # Execute
            results = list(p_plan)
            
            t_ms = (time.perf_counter() - t_start) * 1000
            
            # Output
            if len(results) > 0:
                columns = list(results[0].keys())
                print(format_table(results, columns))
            else:
                print("0 rows returned.")
                
            print(f"\nExecution time: {t_ms:.2f} ms\n")

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"\033[91mError: {e}\033[0m\n")

if __name__ == '__main__':
    main()
