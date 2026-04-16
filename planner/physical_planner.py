from .logical_operators import LogicalOperator, LogicalScan, LogicalFilter, LogicalProject, LogicalJoin, LogicalAggregate, LogicalIndexScan, LogicalLimit
from executor.physical_operators import PhysicalOperator, PhysicalScan, PhysicalFilter, PhysicalProject, PhysicalJoin
from catalog.catalog import Catalog

class PhysicalPlanner:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def plan(self, logical_plan: LogicalOperator) -> PhysicalOperator:
        if isinstance(logical_plan, LogicalScan):
            schema = self.catalog.get_table(logical_plan.table_name)
            return PhysicalScan(
                logical_plan.table_name, 
                logical_plan.columns, 
                schema.file_path,
                logical_plan.alias
            )
        
        elif isinstance(logical_plan, LogicalFilter):
            input_op = self.plan(logical_plan.input)
            return PhysicalFilter(input_op, logical_plan.condition)
        
        elif isinstance(logical_plan, LogicalProject):
            input_op = self.plan(logical_plan.input)
            return PhysicalProject(input_op, logical_plan.expressions, logical_plan.aliases)
        
        elif isinstance(logical_plan, LogicalJoin):
            left_op = self.plan(logical_plan.left)
            right_op = self.plan(logical_plan.right)
            return PhysicalJoin(left_op, right_op, logical_plan.condition)
        
        elif isinstance(logical_plan, LogicalAggregate):
            input_op = self.plan(logical_plan.input)
            from executor.physical_operators import PhysicalHashAggregate
            return PhysicalHashAggregate(input_op, logical_plan.group_by, logical_plan.aggregates)
        
        elif isinstance(logical_plan, LogicalIndexScan):
            schema = self.catalog.get_table(logical_plan.table_name)
            from executor.physical_operators import PhysicalIndexScan
            return PhysicalIndexScan(
                logical_plan.table_name,
                logical_plan.index_column,
                logical_plan.value,
                logical_plan.columns,
                schema.file_path,
                logical_plan.alias
            )
            
        elif isinstance(logical_plan, LogicalLimit):
            input_op = self.plan(logical_plan.input)
            from executor.physical_operators import PhysicalLimit
            return PhysicalLimit(input_op, logical_plan.limit)
            
        else:
            raise NotImplementedError(f"Physical planning for {type(logical_plan)} not implemented")
