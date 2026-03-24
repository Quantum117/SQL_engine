import os
from graphviz import Digraph
from ast_nodes.nodes import ASTNode

class ASTVisualizer:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def visualize(self, ast_root: ASTNode, filename: str = "ast_graph"):
        dot = Digraph(comment='SQL AST')
        dot.attr(rankdir='TB')  # Top to Bottom
        
        # Build the graph
        ast_root.to_dot(dot)
        
        # Render
        output_path = os.path.join(self.output_dir, filename)
        # We use cleanup=True to remove the temporary .gv file, keeping only the image
        dot.render(output_path, format='png', cleanup=True)
        print(f"AST visualization saved to {output_path}.png")
        return f"{output_path}.png"

if __name__ == "__main__":
    from parser.parser import SQLParser
    
    parser = SQLParser()
    sql = """
    SELECT e.name, d.department_name
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    WHERE e.age > 30;
    """
    ast = parser.parse(sql)
    
    visualizer = ASTVisualizer()
    visualizer.visualize(ast, "demo_ast")
