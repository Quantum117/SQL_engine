import os

class PlanVisualizer:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def visualize(self, plan_root, filename: str = "plan"):
        # For now, just print the pretty output to a text file
        # since we are restoring the functional core first.
        # Authentic re-implementation would use graphviz if needed.
        output_path = os.path.join(self.output_dir, f"{filename}.txt")
        with open(output_path, "w") as f:
            f.write(plan_root.pretty())
        print(f"Plan visualization saved to {output_path}")
