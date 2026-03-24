import os
from lark import Lark
from .ast_builder import SQLTransformer

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

if __name__ == "__main__":
    from visualization.visualizer import ASTVisualizer
    parser = SQLParser()
    sql = """
    SELECT name
    FROM employees
    WHERE age > 30;
    """
    ast = parser.parse(sql)
    print("AST Output:")
    print(ast.pretty())

    # visualizer = ASTVisualizer()
    # visualizer.visualize(ast, "demo_ast")
