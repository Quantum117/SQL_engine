# 2.7. Оптимизация на основе правил: Pushdown и Pruning

Для обеспечения приемлемой производительности на миллионах строк, необходимо реализовать фундамент реляционной оптимизации. В движке активно применяются два главных правила: **Predicate Pushdown** и **Column Pruning**. Имплементация данных правил (в `planner/optimizer.py`) позволяет сократить количество операций в памяти на порядки.

### 2.7.1. Проталкивание предикатов (Predicate Pushdown)
Математически, проталкивание предикатов опирается на эквивалентность реляционной алгебры: 
$\sigma_{p}(R \bowtie S) \equiv \sigma_{p}(R) \bowtie S$ (если предикат $p$ ссылается только на атрибуты $R$).

Вместо того чтобы сначала склеить таблицы в огромный массив и затем его отфильтровать, мы фильтруем таблицы перед соединением. В коде движка это реализуется сложной функцией `_push_down`, которая анализирует `LogicalFilter`:

```python
def _push_down(self, cond, node, catalog):
    if isinstance(node, LogicalJoin):
        cond_cols = self._get_referenced_columns(cond)    # Какие колонки в фильтре?
        left_cols = self._get_available_columns(node.left)  # Какие колонки в левой таблице?
        
        # Если предикат опирается исключительно на колонки левой таблицы
        if cond_cols.issubset(left_cols):
            # Рекурсивно толкаем фильтр вниз к левому источнику данных
            new_left = self._push_down(cond, node.left, catalog) or LogicalFilter(node.left, cond)
            # Возвращаем Join с уже отфильтрованным левым входом
            return LogicalJoin(new_left, node.right, node.join_type, node.condition)
```
*Интуиция кода*: Функция инспектирует оператор `JOIN`. С помощью методов `_get_referenced_columns` она узнает, что фильтр требует колонку `oi.category`. Она проверяет, принадлежит ли эта колонка левой ветке (таблице `order_items`). Если да, алгоритм оборачивает сканер данных левой таблицы в новый `LogicalFilter`, освобождая `JOIN` от лишней работы.

### 2.7.2. Усечение столбцов (Column Pruning)
На практике, даже если таблица содержит 100 столбцов, аналитический запрос часто затрагивает только 2-3 (например, агрегация суммы по категориям). `ColumnPruningRule` гарантирует, что движок не будет поднимать с диска "мертвый груз".

```python
def _prune(self, op: LogicalOperator, required: Set[str]) -> LogicalOperator:
    # ... обработка верхних уровней ...
    elif isinstance(op, LogicalScan):
        new_cols = []
        for col in op.columns:
            # Оставляем столбец, только если он в списке 'required' (нужен для SELECT или WHERE)
            if col in required or f"{op.alias}.{col}" in required:
                new_cols.append(col)
                
        # Модифицируем объект сканирования in-place 
        op.columns = new_cols if new_cols else [op.columns[0]]
        return op
```
*Интуиция метода*: Оптимизатор спускается от корня плана (`SELECT`) вниз к `FROM`. Попутно он "собирает" в аргумент `required` все встреченные имена колонок (из `WHERE`, из агрегатов, из самого `SELECT`). Дойдя до "дна" дерева (до `LogicalScan`), он перезаписывает свойство `op.columns` таким образом, чтобы парсер CSV-файлов или генератор NumPy считывал строго усеченный список колонок.

Вместе с правилом свертки констант (`ConstantFoldingRule`) и выбора индексов (`IndexRule`), эти алгебраические трансформации подготавливают идеальный декларативный план для физического исполнителя.
