from .logical_operators import LogicalOperator, LogicalScan, LogicalFilter, LogicalProject, LogicalJoin, LogicalAggregate, LogicalIndexScan, LogicalLimit
from executor.vectorized_operators import VectorizedOperator, VectorizedScan, VectorizedFilter, VectorizedProject, VectorizedHashJoin, VectorizedHashAggregate, VectorizedIndexScan
from catalog.catalog import Catalog

class VectorizedPlanner:
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def plan(self, logical_plan: LogicalOperator) -> VectorizedOperator:
        if isinstance(logical_plan, LogicalScan):
            schema = self.catalog.get_table(logical_plan.table_name)
            return VectorizedScan(
                logical_plan.table_name, 
                logical_plan.columns, 
                schema.file_path,
                logical_plan.alias
            )
        
        elif isinstance(logical_plan, LogicalFilter):
            input_op = self.plan(logical_plan.input)
            return VectorizedFilter(input_op, logical_plan.condition)
        
        elif isinstance(logical_plan, LogicalProject):
            input_op = self.plan(logical_plan.input)
            return VectorizedProject(input_op, logical_plan.expressions, logical_plan.aliases)
        
        elif isinstance(logical_plan, LogicalJoin):
            left_op = self.plan(logical_plan.left)
            right_op = self.plan(logical_plan.right)
            # We assume equality join on columns for VectorizedHashJoin
            return VectorizedHashJoin(left_op, right_op, logical_plan.condition)
        
        elif isinstance(logical_plan, LogicalAggregate):
            input_op = self.plan(logical_plan.input)
            return VectorizedHashAggregate(input_op, logical_plan.group_by, logical_plan.aggregates)
        
        elif isinstance(logical_plan, LogicalIndexScan):
            schema = self.catalog.get_table(logical_plan.table_name)
            return VectorizedIndexScan(
                logical_plan.table_name,
                logical_plan.index_column,
                logical_plan.value,
                logical_plan.columns,
                schema.file_path,
                logical_plan.alias
            )
            
        elif isinstance(logical_plan, LogicalLimit):
            input_op = self.plan(logical_plan.input)
            from executor.vectorized_operators import VectorizedLimit
            return VectorizedLimit(input_op, logical_plan.limit)
            
        else:
            raise NotImplementedError(f"Vectorized planning for {type(logical_plan)} not implemented")
