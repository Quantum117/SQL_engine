import csv
import random

def generate_users(n=1000):
    with open('bench_users.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'age'])
        for i in range(1, n + 1):
            writer.writerow([i, f"User_{i}", random.randint(18, 90)])
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
    generate_users()
    generate_products()
    generate_orders()
