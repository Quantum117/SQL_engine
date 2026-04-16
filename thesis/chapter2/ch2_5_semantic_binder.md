# 2.5. Процесс связывания (Binding) и разрешение идентификаторов

Если парсер строит синтаксическое дерево (AST) исходя только из текста, то этап **Semantic Binding** (семантическое связывание) отвечает за "оживление" этого дерева. Binder сопоставляет абстрактные имена с реальными схемами данных из Системного Каталога, генерируя первичный логический план.

### 2.5.1. Анализ кода: Перевод AST в логические операторы

Основной метод `bind_select` в классе `Binder` (`planner/binder.py`) рекурсивно обходит узлы AST и заменяет их на узлы `LogicalOperator`. 

```python
def bind_table(self, table_ref: Any) -> Any:
    if isinstance(table_ref, TableRef):
        # 1. Запрашиваем схему из каталога
        schema = self.catalog.get_table(table_ref.table_name)
        alias = table_ref.alias or table_ref.table_name
        
        # 2. Регистрируем алиас в текущей области видимости
        if alias in self.current_scope:
            raise SemanticError(f"Duplicate table alias: {alias}")
        self.current_scope[alias] = schema
        
        # 3. Возвращаем логический оператор сканирования
        return LogicalScan(table_ref.table_name, list(schema.columns), alias)
```

*Интуиция кода*: Когда парсер видит конструкцию `FROM users AS u`, он создает узел `TableRef(table="users", alias="u")`. На этапе Binding мы запрашиваем каталог: существует ли таблица `users`? Если да, мы создаем локальную область видимости (scope) `{'u': users_schema}`. Это позволяет следующему этапу (обработке `WHERE u.age > 18`) корректно понять, что означает префикс `u.`. Если таблица не найдена, произойдет досрочное прерывание выполнения запроса с выбросом исключения `SemanticError`, что предотвращает бессмысленную работу исполнителя.

### 2.5.2. Разрешение пространств имен (Namespace Resolution)

Самая сложная задача связывателя — обработка столбцов в соединениях без явного указания таблицы. Например:
`SELECT id, name FROM users JOIN orders ON users.id = user_id`.

Когда движок встречает узел `ColumnRef(name='user_id', table=None)`, метод `resolve_column` выполняет поиск:
```python
def resolve_column(self, col: ColumnRef) -> ColumnRef:
    if col.table is None:
        matches = []
        for alias, schema in self.current_scope.items():
            if col.name in schema.columns:
                matches.append(alias)
        
        if not matches:
             raise SemanticError(f"Column '{col.name}' not found")
        if len(matches) > 1:
             raise SemanticError(f"Column '{col.name}' is ambiguous")
             
        return ColumnRef(table=matches[0], name=col.name)
```
*Интуиция кода*: Binder перебирает **все** доступные алиасы в текущей `scope`. Если `user_id` есть только в `orders`, он незаметно для пользователя "дописывает" `orders.user_id`. Если же и в `users`, и в `orders` есть колонка `user_id` с одинаковым именем, будет перехвачена неоднозначность (ambiguity), что соответствует стандарту работы настоящих СУБД (например, ошибка `ambiguous column name` в PostgreSQL).
