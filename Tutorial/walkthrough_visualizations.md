# Walkthrough: SQL Engine Visualization & Profiling 🎨⚡

In this step, we've transformed the SQL engine from a text-only system into a visually rich platform with professional-grade diagnostics. We implemented three distinct visualization modes to aid in debugging, performance analysis, and architectural education.

---

## 1. Physical Execution DAG ⚡
Instead of reading nested text, you can now generate a flow diagram of the actual physical operators.

- **Visual Features**: Operator-specific icons (📋 Scan, 🔍 Filter, 🔗 Join, Σ Aggregate, ✂️ Project) and distinct shapes.
- **Data Flow**: Arrows are labeled with the actual number of rows processed by each operator.
- **Payoff**: Instantly see the structure of your physical execution plan.

![Physical Execution DAG](file:///c:/Users/Lenovo/PycharmProjects/SQL_engine/output/demo_physical_dag.png)

---

## 2. Model Comparison (Volcano vs. Vectorized) 📦
A conceptual diagram that explains the "Why" behind the engine's performance.

- **Volcano Side**: Shows individual rows (🔘) flowing through the system, illustrating high function-call overhead.
- **Vectorized Side**: Shows bulk batches (📦) flowing through the system, illustrating SIMD-friendly processing.
- **Payoff**: Perfect for explaining the performance leap of the vectorized model in a thesis or presentation.

![Model Movement Comparison](file:///c:/Users/Lenovo/PycharmProjects/SQL_engine/output/model_movement_comparison.png)

---

## 3. Operator Performance Heatmap 🔥
We've built a real-time profiler directly into the engine.

- **Exclusive Timing**: Every operator now tracks its own execution time (excluding children).
- **Color Gradient**: Nodes glow **Bright Red** when they are the primary bottleneck and stay **Cool Green** when they are efficient.
- **Payoff**: Identifying bottlenecks (like expensive JOINS) takes seconds instead of hours of debugging.

![Performance Heatmap](file:///c:/Users/Lenovo/PycharmProjects/SQL_engine/output/performance_heatmap.png)

---

## Technical Implementation Details

### Timing Instrumentation
We modified the core `PhysicalOperator` and `VectorizedOperator` classes to include timing stats.
```python
# executor/physical_operators.py
self.stats = {"processed_rows": 0, "time_ms": 0.0}

# Instrumentation in __next__
start = time.perf_counter()
try:
    # ... operator logic ...
finally:
    self.stats["time_ms"] += (time.perf_counter() - start) * 1000
```

### Visualizer Classes
All visualization logic is encapsulated in [visualization/visualizer.py](file:///c:/Users/Lenovo/PycharmProjects/SQL_engine/visualization/visualizer.py).
- `PhysicalPlanVisualizer`: For structural DAGs.
- `ModelComparisonVisualizer`: For the architectural comparison.
- `HeatmapVisualizer`: For the performance-based coloring.

---

> [!TIP]
> You can try all these features using the newly created scripts in the `scripts/` directory:
> - `scripts/physical_dag_demo.py`
> - `scripts/generate_comparison.py`
> - `scripts/heatmap_demo.py`
