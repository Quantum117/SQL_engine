import os
import html
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

class ConsoleVisualizer:
    """Instant tree view for the terminal."""
    def visualize(self, plan_root, title: str = "Query Plan"):
        print("\n" + "=" * 60)
        print(f" {title.upper()} ".center(60, "="))
        print("=" * 60)
        print(plan_root.to_terminal().strip())
        print("=" * 60 + "\n")

class LogicalPlanVisualizer:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def visualize(self, plan_root, filename: str = "logical_plan"):
        dot = Digraph(comment='SQL Logical Plan')
        # Use a professional, compact layout
        dot.attr(rankdir='TB')
        dot.attr('node', fontname='Helvetica,Arial,sans-serif', fontsize='11')
        
        # Build the graph
        plan_root.to_dot(dot)
        
        # Render
        output_path = os.path.join(self.output_dir, filename)
        dot.render(output_path, format='png', cleanup=True)
        print(f"Logical Plan visualization saved to {output_path}.png")
        return f"{output_path}.png"

class MermaidVisualizer:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def visualize(self, plan_root, filename: str = "logical_plan_mermaid"):
        lines = ["graph TD"]
        plan_root.to_mermaid(lines)
        
        content = "\n".join(lines)
        output_path = os.path.join(self.output_dir, f"{filename}.md")
        
        with open(output_path, "w") as f:
            f.write("```mermaid\n")
            f.write(content)
            f.write("\n```")
            
        print(f"Mermaid visualization saved to {output_path}")
        return output_path

if __name__ == "__main__":
    from parser.parser import SQLParser
    from planner.planner import LogicalPlanner
    from catalog.catalog import Catalog, TableSchema
    
    # Setup
    catalog = Catalog()
    catalog.register_table(TableSchema.from_lists("employees", ["id", "name", "age", "department_id"], ["INT", "STR", "INT", "INT"]))
    catalog.register_table(TableSchema.from_lists("departments", ["id", "department_name"], ["INT", "STR"]))
    
    parser = SQLParser()
    planner = LogicalPlanner(catalog)
    
    sql = """
    SELECT e.name, d.department_name
    FROM employees e
    JOIN departments d ON e.department_id = d.id
    WHERE e.age > 30;
    """
    ast = parser.parse(sql)
    plan = planner.plan(ast)
    
    # Test Console Visualizer
    con_vis = ConsoleVisualizer()
    con_vis.visualize(plan, "Demo Query Plan")
    
    # Test Logical Visualizer (Structured)
    # Note: Requires Graphviz system library
    log_vis = LogicalPlanVisualizer()
    log_vis.visualize(plan, "test_logical_structured")

class PhysicalPlanVisualizer:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Style mapping for operators
        # format: (Icon, Shape, Color)
        self.operator_styles = {
            "PhysicalScan": ("📋", "box", "lightblue"),
            "VectorizedScan": ("📋", "box", "lightblue"),
            "PhysicalIndexScan": ("📋", "box", "cyan"),
            "VectorizedIndexScan": ("📋", "box", "cyan"),
            "PhysicalFilter": ("🔍", "invhouse", "lightyellow"),
            "VectorizedFilter": ("🔍", "invhouse", "lightyellow"),
            "PhysicalProject": ("✂️", "parallelogram", "lightgrey"),
            "VectorizedProject": ("✂️", "parallelogram", "lightgrey"),
            "PhysicalJoin": ("🔗", "diamond", "lightcoral"),
            "PhysicalHashJoin": ("🔗", "diamond", "lightcoral"),
            "VectorizedHashJoin": ("🔗", "diamond", "lightcoral"),
            "PhysicalHashAggregate": ("Σ", "trapezium", "lightgreen"),
            "VectorizedHashAggregate": ("Σ", "trapezium", "lightgreen"),
        }

    def visualize(self, plan_root, filename: str = "physical_execution_dag"):
        dot = Digraph(comment='SQL Physical Execution DAG')
        dot.attr(rankdir='BT')  # Bottom to Top (Leaf to Root) looks better for data flow
        dot.attr('node', fontname='Segoe UI, Helvetica, Arial', fontsize='11')
        
        self._traverse(plan_root, dot)
        
        output_path = os.path.join(self.output_dir, filename)
        dot.render(output_path, format='png', cleanup=True)
        print(f"Physical Execution DAG saved to {output_path}.png")
        return f"{output_path}.png"

    def _traverse(self, operator, dot):
        node_id = str(id(operator))
        class_name = operator.__class__.__name__
        
        # Get style or default
        icon, shape, color = self.operator_styles.get(class_name, ("⚙️", "ellipse", "white"))
        
        # Get stats
        stats = operator.get_stats()
        rows = stats.get("processed_rows", 0)
        
        # Build Label
        label = f'<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="2">'
        label += f'<TR><TD><FONT POINT-SIZE="14">{icon}</FONT></TD></TR>'
        label += f'<TR><TD><B>{class_name}</B></TD></TR>'
        label += f'<TR><TD><FONT COLOR="#666666">Rows: {rows}</FONT></TD></TR>'
        label += '</TABLE>>'
        
        dot.node(node_id, label, shape=shape, style="filled", fillcolor=color)
        
        # Find children
        children = []
        if hasattr(operator, 'input'):
            children.append(operator.input)
        if hasattr(operator, 'left'):
            children.append(operator.left)
        if hasattr(operator, 'right'):
            children.append(operator.right)
            
        for child in children:
            if child:
                child_id = self._traverse(child, dot)
                # Arrow from child to parent with row count label
                child_stats = child.get_stats()
                child_rows = child_stats.get("processed_rows", 0)
                dot.edge(child_id, node_id, label=f" {child_rows} rows")
        
        return node_id

class ModelComparisonVisualizer:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def visualize_comparison(self, filename: str = "model_comparison"):
        dot = Digraph(comment='Volcano vs Vectorized Data Movement', engine='dot')
        # Use a wide layout to fit both models side-by-side
        dot.attr(rankdir='LR', ranksep='1.2', nodesep='0.5')
        dot.attr('node', fontname='Segoe UI, Helvetica, Arial', fontsize='12')
        dot.attr('edge', fontname='Segoe UI, Helvetica, Arial', fontsize='10')

        # Volcano Side
        with dot.subgraph(name='cluster_volcano') as c:
            c.attr(label='Volcano Model (Row-at-a-Time)', labelloc='t', fontsize='16', fontcolor='#e74c3c', style='dashed', color='#e74c3c', penwidth='2')
            
            c.node('v_scan', 'PhysicalScan\n(Table)', shape='box', style='filled', fillcolor='#fdedec')
            c.node('v_filter', 'PhysicalFilter\n(Filter)', shape='invhouse', style='filled', fillcolor='#fdedec')
            c.node('v_project', 'PhysicalProject\n(Project)', shape='parallelogram', style='filled', fillcolor='#fdedec')
            
            # Edges with single rows
            row_label = '<<TABLE BORDER="0" CELLPADDING="0" CELLSPACING="0"><TR><TD><FONT POINT-SIZE="20">🔘</FONT></TD></TR><TR><TD>Individual Row</TD></TR></TABLE>>'
            c.edge('v_scan', 'v_filter', label=row_label, color='#e74c3c')
            c.edge('v_filter', 'v_project', label=row_label, color='#e74c3c')
            
            c.node('v_note', 'Overhead:\nLow CPU Cache Utility\nHigh Function Call Count', shape='plaintext', fontcolor='#c0392b', fontsize='10')

        # Vectorized Side
        with dot.subgraph(name='cluster_vectorized') as c:
            c.attr(label='Vectorized Model (Batch-at-a-Time)', labelloc='t', fontsize='16', fontcolor='#27ae60', style='dashed', color='#27ae60', penwidth='2')
            
            c.node('vec_scan', 'VectorizedScan\n(Table)', shape='box', style='filled', fillcolor='#e9f7ef')
            c.node('vec_filter', 'VectorizedFilter\n(Filter)', shape='invhouse', style='filled', fillcolor='#e9f7ef')
            c.node('vec_project', 'VectorizedProject\n(Project)', shape='parallelogram', style='filled', fillcolor='#e9f7ef')
            
            # Edges with batches
            batch_label = '<<TABLE BORDER="0" CELLPADDING="0" CELLSPACING="0"><TR><TD><FONT POINT-SIZE="24">📦</FONT></TD></TR><TR><TD><B>Data Batch (1024 Rows)</B></TD></TR></TABLE>>'
            c.edge('vec_scan', 'vec_filter', label=batch_label, color='#27ae60', penwidth='2.5')
            c.edge('vec_filter', 'vec_project', label=batch_label, color='#27ae60', penwidth='2.5')
            
            c.node('vec_note', 'Efficiency:\nNumPy / SIMD Optimized\nReduced Context Switching', shape='plaintext', fontcolor='#1e8449', fontsize='10')

        # Center Title / Branding
        dot.attr(label='\nSQL Engine Execution Model Comparison\nPhysical Data Movement Strategy', labelloc='b', fontsize='20', fontname='Segoe UI Semibold')

        output_path = os.path.join(self.output_dir, filename)
        dot.render(output_path, format='png', cleanup=True)
        print(f"Model comparison visualization saved to {output_path}.png")
        return f"{output_path}.png"

class HeatmapVisualizer:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def visualize(self, plan_root, filename: str = "performance_heatmap"):
        dot = Digraph(comment='SQL Operator Performance Heatmap')
        dot.attr(rankdir='BT')
        dot.attr('node', fontname='Segoe UI, Helvetica, Arial', fontsize='11')
        
        # 1. First pass: Collect all stats and calculate total time
        node_stats = {}
        self._collect_stats(plan_root, node_stats)
        total_time = sum(s['exclusive'] for s in node_stats.values())
        if total_time == 0: total_time = 0.001 # Avoid division by zero
        
        # 2. Second pass: Build the graph with colors
        self._build_heatmap(plan_root, dot, node_stats, total_time)
        
        output_path = os.path.join(self.output_dir, filename)
        dot.render(output_path, format='png', cleanup=True)
        print(f"Performance Heatmap saved to {output_path}.png")
        return f"{output_path}.png"

    def _collect_stats(self, operator, node_stats):
        node_id = str(id(operator))
        stats = operator.get_stats()
        inclusive_time = stats.get("time_ms", 0)
        
        # Find children to calculate exclusive time
        children = []
        if hasattr(operator, 'input'): children.append(operator.input)
        if hasattr(operator, 'left'): children.append(operator.left)
        if hasattr(operator, 'right'): children.append(operator.right)
        
        child_inclusive_sum = 0
        for child in children:
            if child:
                self._collect_stats(child, node_stats)
                child_inclusive_sum += child.get_stats().get("time_ms", 0)
        
        exclusive_time = max(0, inclusive_time - child_inclusive_sum)
        node_stats[node_id] = {
            "inclusive": inclusive_time,
            "exclusive": exclusive_time,
            "rows": stats.get("processed_rows", 0)
        }

    def _build_heatmap(self, operator, dot, node_stats, total_time):
        node_id = str(id(operator))
        class_name = operator.__class__.__name__
        stats = node_stats[node_id]
        
        pct = stats['exclusive'] / total_time
        color = self._get_color(pct)
        
        # Build Label
        label = f'<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="2">'
        label += f'<TR><TD><B>{class_name}</B></TD></TR>'
        label += f'<TR><TD>Time: {stats["exclusive"]:.2f} ms</TD></TR>'
        label += f'<TR><TD><FONT POINT-SIZE="9" COLOR="#444444">({pct*100:.1f}% of total)</FONT></TD></TR>'
        label += f'<TR><TD><FONT POINT-SIZE="9" COLOR="#444444">{stats["rows"]} rows</FONT></TD></TR>'
        label += '</TABLE>>'
        
        # Shape based on operator type
        shape = "box"
        if "Filter" in class_name: shape = "invhouse"
        elif "Project" in class_name: shape = "parallelogram"
        elif "Join" in class_name: shape = "diamond"
        elif "Aggregate" in class_name: shape = "trapezium"
        
        dot.node(node_id, label, shape=shape, style="filled", fillcolor=color)
        
        # Children
        children = []
        if hasattr(operator, 'input'): children.append(operator.input)
        if hasattr(operator, 'left'): children.append(operator.left)
        if hasattr(operator, 'right'): children.append(operator.right)
        
        for child in children:
            if child:
                child_id = str(id(child))
                dot.edge(child_id, node_id)
                self._build_heatmap(child, dot, node_stats, total_time)

    def _get_color(self, pct):
        # pct is 0 to 1. Gradient from Green (#2ecc71) to Red (#e74c3c)
        # We'll use a simple linear interpolation for simplicity
        if pct < 0.3: # Low: Greenish
            r, g, b = 46 + (241-46)*pct*3, 204, 113
        elif pct < 0.7: # Medium: Yellowish
            r, g, b = 241, 204 - (204-196)*(pct-0.3)*2.5, 113 - (113-15)*(pct-0.3)*2.5
        else: # High: Reddish
            r, g, b = 241 - (241-231)*(pct-0.7)*3.3, 196 - (196-76)*(pct-0.7)*3.3, 15 + (60-15)*(pct-0.7)*3.3
        
        return f"#{int(r):02x}{int(g):02x}{int(b):02x}"
