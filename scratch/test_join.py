from executor.physical_operators import PhysicalScan, PhysicalJoin
from ast_nodes.nodes import ColumnRef, BinaryExpression, Literal
import os

def test():
    # Create dummy files
    with open("t1.csv", "w") as f:
        f.write("id,v\n1,10\n2,20\n")
    with open("t2.csv", "w") as f:
        f.write("id,x\n1,100\n3,300\n")
        
    s1 = PhysicalScan("t1", ["id", "v"], "t1.csv")
    s2 = PhysicalScan("t2", ["id", "x"], "t2.csv")
    
    cond = BinaryExpression(ColumnRef("id", "t1"), "=", ColumnRef("id", "t2"))
    join = PhysicalJoin(s1, s2, cond)
    
    print("Starting join...")
    for row in join:
        print(f"Row: {row}")

if __name__ == "__main__":
    test()
