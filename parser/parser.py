import os
from lark import Lark
try:
    from .ast_builder import SQLTransformer
except (ImportError, ValueError):
    from ast_builder import SQLTransformer

class SQLParser:
    def __init__(self):
        grammar_path = os.path.join(os.path.dirname(__file__), 'grammar.lark')
        with open(grammar_path, 'r') as f:
            self.grammar = f.read()
        self.lark = Lark(self.grammar, start='select_statement', parser='lalr')
        self.transformer = SQLTransformer()

    def parse(self, sql_text: str):
        # Basic cleanup: remove leading/trailing whitespace and ensure it ends with semicolon if missing
        sql_text = sql_text.strip()
        if not sql_text.endswith(';'):
            sql_text += ';'
            
        tree = self.lark.parse(sql_text)
        return self.transformer.transform(tree)

    def lex(self, sql_text: str):
        sql_text = sql_text.strip()
        if not sql_text.endswith(';'):
            sql_text += ';'
        return list(self.lark.lex(sql_text))

if __name__ == "__main__":
    from visualization.visualizer import ASTVisualizer
    parser = SQLParser()
    sql_simple = """
    SELECT name, SUM(salary)
    FROM employees
    WHERE dept_id = 10
    GROUP BY name
    ORDER BY SUM(salary) DESC;

    
    """
    sql_complex = """
    SELECT 
    u.username,
    u.city,
    COUNT(o.order_id) AS total_orders,
    SUM(oi.price * oi.quantity) AS total_spent,
    AVG(oi.price * oi.quantity) AS avg_item_value
    FROM users u
    JOIN orders o ON u.user_id = o.user_id
    JOIN order_items oi ON o.order_id = oi.order_id
    WHERE oi.category = 'Electronics'
    GROUP BY u.username, u.city
    HAVING SUM(oi.price * oi.quantity) > 500
    ORDER BY total_spent DESC;"""
    ast_simple = parser.parse(sql_simple)
    ast_complex = parser.parse(sql_complex)

    print("AST Output:")
    print(ast_simple.pretty())

    visualizer = ASTVisualizer()
    visualizer.visualize(ast_complex, "demo_ast_complex")
    visualizer.visualize(ast_simple, "demo_ast_simple")
