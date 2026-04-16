import os
import sys
import time
import pandas as pd
import streamlit as st

st.set_page_config(page_title="SQL Engine Pro", page_icon="⚡", layout="wide")

# Add root folder to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from parser.parser import SQLParser
from catalog.catalog import Catalog, TableSchema
from planner.planner import LogicalPlanner
from planner.optimizer import LogicalOptimizer, PredicatePushdownRule, ColumnPruningRule
from planner.vectorized_planner import VectorizedPlanner
from planner.physical_planner import PhysicalPlanner
from visualization.visualizer import PhysicalPlanVisualizer, ASTVisualizer, HeatmapVisualizer, LogicalPlanVisualizer

@st.cache_resource
def load_engine():
    catalog = Catalog()
    path_data = os.path.join("benchmarks", "data")
    
    catalog.register_table(TableSchema.from_lists("users", ["id", "name", "age"], ["INT", "STR", "INT"], file_path=os.path.join(path_data, "showcase_users.csv")))
    catalog.register_table(TableSchema.from_lists("products", ["id", "name", "category", "status"], ["INT", "STR", "STR", "STR"], file_path=os.path.join(path_data, "showcase_products.csv")))
    catalog.register_table(TableSchema.from_lists("orders", ["id", "user_id", "product_id", "amount"], ["INT", "INT", "INT", "FLOAT"], file_path=os.path.join(path_data, "showcase_orders.csv")))
    
    # New Standard Datasets
    catalog.register_table(TableSchema.from_lists("titanic", ["PassengerId", "Survived", "Pclass", "Name", "Sex", "Age", "SibSp", "Parch", "Ticket", "Fare", "Cabin", "Embarked"], ["INT", "INT", "INT", "STR", "STR", "INT", "INT", "INT", "STR", "FLOAT", "STR", "STR"], file_path=os.path.join(path_data, "titanic.csv")))
    catalog.register_table(TableSchema.from_lists("pokemon", ["id", "name", "type_1", "type_2", "total", "hp", "attack", "defense", "sp_atk", "sp_def", "speed", "generation", "legendary"], ["INT", "STR", "STR", "STR", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT"], file_path=os.path.join(path_data, "pokemon.csv")))
        
    parser = SQLParser()
    l_planner = LogicalPlanner(catalog)
    v_planner = VectorizedPlanner(catalog)
    p_planner = PhysicalPlanner(catalog)
    
    optimizer = LogicalOptimizer(catalog)
    optimizer.add_rule(PredicatePushdownRule())
    optimizer.add_rule(ColumnPruningRule())
    
    return catalog, parser, l_planner, optimizer, p_planner, v_planner

st.title("⚡ Дашборд Python SQL Engine")
st.markdown("Вводите SQL-запросы ниже. Движок распарсит, оптимизирует и выполнит их нативно.")

catalog, parser, l_planner, optimizer, p_planner, v_planner = load_engine()

# Sidebar: Database Schema
st.sidebar.title("🗄️ Обозреватель БД")
for table_name, schema in catalog.tables.items():
    with st.sidebar.expander(f"📁 {table_name}"):
        st.caption(f"Файл: {os.path.basename(schema.file_path)}")
        for col_name, col_type in schema.columns.items():
            st.markdown(f"🔸 `{col_name}` : **{col_type}**")

example_queries = {
    "Пользовательский Запрос": "SELECT u.name, o.amount\nFROM users u \nJOIN orders o ON u.id = o.user_id\nWHERE u.age > 40\nLIMIT 5;",
    "Анализ выживаемости (Titanic)": "SELECT Sex, SUM(Survived), COUNT(PassengerId)\nFROM titanic\nWHERE Age > 18\nGROUP BY Sex;",
    "Самые сильные Покемоны (Pokemon)": "SELECT name, type_1, attack, defense, total\nFROM pokemon\nWHERE legendary = 1\nLIMIT 10;",
    "Простое чтение (Scan)": "SELECT id, name FROM users LIMIT 10;",
    "Стресс-тест агрегации (Vectorized)": "SELECT category, SUM(amount)\nFROM orders o\nJOIN products p ON o.product_id = p.id\nGROUP BY category;",
    "Сложный фильтр + LIMIT": "SELECT o.id, u.name, o.amount\nFROM orders o\nJOIN users u ON o.user_id = u.id\nWHERE o.amount > 500\nLIMIT 5;"
}
selected_example = st.selectbox("📚 Загрузить пример запроса", list(example_queries.keys()))
sql = st.text_area("SQL-Запрос", value=example_queries[selected_example], height=150)

col1, col2 = st.columns([1, 4])
with col1:
    execute_bt = st.button("🚀 Выполнить запрос", type="primary")
with col2:
    engine_type = st.radio("Механизм выполнения:", ["Volcano (Построчный)", "Vectorized (Векторный NumPy)"], horizontal=True)

if execute_bt:
    try:
        t_start = time.perf_counter()
        
        # Pipeline
        ast = parser.parse(sql)
        l_plan = l_planner.plan(ast)
        o_plan = optimizer.optimize(l_plan)
        
        results_df = None
        if engine_type == "Volcano (Row-based)":
            p_plan = p_planner.plan(o_plan)
            active_plan = p_plan
            results = list(p_plan)
            if results: results_df = pd.DataFrame(results)
        else:
            v_plan = v_planner.plan(o_plan)
            active_plan = v_plan
            dfs = []
            while True:
                batch = v_plan.next_batch()
                if batch is None: break
                dfs.append(pd.DataFrame(batch))
            if dfs: results_df = pd.concat(dfs, ignore_index=True)
        
        t_ms = (time.perf_counter() - t_start) * 1000
        
        st.success(f"Запрос успешно выполнен за {t_ms:.2f} мс")
        
        if results_df is not None and not results_df.empty:
            import numpy as np
            import matplotlib.pyplot as plt

            num_cols = results_df.select_dtypes(include=[np.number]).columns
            if len(num_cols) > 0:
                st.markdown("### Распределение данных ")
                # Wrap responsive cols row by row
                max_cols_per_row = 6
                for i in range(0, len(num_cols), max_cols_per_row):
                    row_keys = num_cols[i:i + max_cols_per_row]
                    cols_ui = st.columns(max_cols_per_row)
                    for j, c in enumerate(row_keys):
                        with cols_ui[j]:
                            fig, ax = plt.subplots(figsize=(2.5, 1.5))
                            ax.hist(results_df[c].dropna(), bins=10, color="#FF4B4B", edgecolor='white', linewidth=0.5)
                            ax.set_yticks([])
                            ax.tick_params(axis='x', labelsize=8, colors="gray")
                            for spine in ax.spines.values(): spine.set_visible(False)
                            fig.patch.set_alpha(0.0) # Transparent background
                            ax.set_facecolor("none")
                            st.markdown(f"<div style='text-align: center; font-size: 13px; color: lightgray;'><b>{c}</b></div>", unsafe_allow_html=True)
                            st.pyplot(fig)
                            plt.close(fig)

            st.dataframe(results_df, use_container_width=True)
            
            # Download Button (use original df so sparklines aren't downloaded)
            csv_data = results_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Скачать как CSV",
                data=csv_data,
                file_name="query_results.csv",
                mime="text/csv"
            )
        else:
            st.info("Запрос вернул 0 строк.")
            
        # Draw visualizations using tabs
        st.subheader("Визуализации выполнения")
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["[1] Лексер (Токены)", "[2] AST-Дерево", "[3] Логический План", "[4] Физический план DAG", "[5] Тепловая карта"])
        
        with tab1:
            st.markdown("### Лексический Анализ (Токенизация)")
            tokens = parser.lex(sql)
            df_tokens = pd.DataFrame([
                {"Тип Токена": t.type, "Значение": t.value, "Строка": t.line, "Колонка": t.column} 
                for t in tokens
            ])
            st.dataframe(df_tokens, use_container_width=True)

        with tab2:
            ast_vis = ASTVisualizer()
            ast_path = ast_vis.visualize(ast, "ui_ast_plan")
            if os.path.exists(ast_path):
                st.image(ast_path, caption="Сгенерированное AST (из потока токенов)")
                
        with tab3:
            log_vis = LogicalPlanVisualizer()
            l_path = log_vis.visualize(l_plan, "ui_raw_logical_plan")
            o_path = log_vis.visualize(o_plan, "ui_opt_logical_plan")
            
            st.markdown("### Сравнение Логических Планов")
            st.markdown("Слева показан сырой план. Справа — оптимизированный ('Fast') план.")
            
            c1, c2 = st.columns(2)
            with c1:
                if os.path.exists(l_path):
                    st.image(l_path, caption="Сырой логический план (До оптимизации)")
            with c2:
                if os.path.exists(o_path):
                    st.image(o_path, caption="Оптимизированный логический план (После оптимизации)")
                
        with tab4:
            visualizer = PhysicalPlanVisualizer()
            output_path = visualizer.visualize(active_plan, "ui_query_plan")
            if os.path.exists(output_path):
                st.image(output_path, caption="Оптимизированный DAG-граф выполнения")

        with tab5:
            h_vis = HeatmapVisualizer()
            h_path = h_vis.visualize(active_plan, "ui_heatmap")
            if os.path.exists(h_path):
                st.image(h_path, caption="Тепловая карта: узкие места выполнения (Heatmap)")

    except Exception as e:
        st.error(f"Ошибка выполнения запроса: {e}")
