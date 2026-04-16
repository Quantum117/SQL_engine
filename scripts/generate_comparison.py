import sys
import os

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from visualization.visualizer import ModelComparisonVisualizer

def main():
    print("Generating Model Comparison Visualization...")
    viz = ModelComparisonVisualizer(output_dir="output")
    output_png = viz.visualize_comparison("model_movement_comparison")
    print(f"Success! Visualization saved to: {output_png}")

if __name__ == "__main__":
    main()
