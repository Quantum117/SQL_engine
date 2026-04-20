import csv
import random

def generate_users(n=1000):
    first_names = ["Александр", "Иван", "Максим", "Дмитрий", "Сергей", "Михаил", "Николай", "Андрей", "Артем", "Алексей", "Elena", "Maria", "Anna", "Olga", "Dmitry", "Alex", "John", "Sarah", "Emily", "Michael"]
    last_names = ["Иванов", "Петров", "Сидоров", "Смирнов", "Кузнецов", "Волков", "Лебедев", "Козлов", "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
    
    with open('bench_users.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'age'])
        for i in range(1, n + 1):
            name = f"{random.choice(first_names)} {random.choice(last_names)}"
            writer.writerow([i, name, random.randint(18, 90)])
    print(f"Generated {n} users.")

def generate_products(n=500):
    with open('bench_products.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'category', 'status'])
        categories = ['Electronics', 'Home', 'Garden', 'Books', 'Toys']
        statuses = ['active', 'inactive']
        for i in range(1, n + 1):
            writer.writerow([i, f"Product_{i}", random.choice(categories), random.choice(statuses)])
    print(f"Generated {n} products.")

def generate_orders(n=5000, n_users=1000, n_products=500):
    with open('bench_orders.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'user_id', 'product_id', 'amount'])
        for i in range(1, n + 1):
            writer.writerow([i, random.randint(1, n_users), random.randint(1, n_products), round(random.uniform(10.0, 500.0), 2)])
    print(f"Generated {n} orders.")

if __name__ == "__main__":
    import os
    os.makedirs(os.path.join("benchmarks", "data"), exist_ok=True)
    # Generate Stress Test Data (Scaled down 10x)
    generate_users(10000)
    generate_products(100)
    generate_orders(20000, 10000, 100)
    
    # Move files to benchmarks/data
    import shutil
    for f in ['bench_users.csv', 'bench_products.csv', 'bench_orders.csv']:
        if os.path.exists(f):
            dest = os.path.join("benchmarks", "data", f)
            shutil.move(f, dest)
            print(f"Moved {f} to {dest}")
