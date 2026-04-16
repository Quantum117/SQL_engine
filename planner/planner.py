from parser.parser import SQLParser
from ast_nodes.nodes import SelectStatement
from .binder import Binder
from .logical_operators import LogicalOperator
from catalog.catalog import Catalog

class LogicalPlanner:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        self.binder = Binder(catalog)

    def plan(self, ast) -> LogicalOperator:
        if isinstance(ast, SelectStatement):
            plan, _ = self.binder.bind_select(ast)
            return plan
        else:
            raise NotImplementedError("Only SELECT statements are supported currently")
