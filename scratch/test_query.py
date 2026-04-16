from parser.parser import SQLParser

sql = """
SELECT 
    u.username,
    u.city,
    COUNT(o.order_id) AS total_orders,
    SUM(oi.price * oi.quantity) AS total_spent,
    AVG(oi.price * oi.quantity) AS avg_item_value
FROM users u
JOIN orders o ON u.user_id = o.user_id
JOIN order_items oi ON o.order_id = oi.order_id
WHERE oi.category = 'Electronics'
GROUP BY u.username, u.city
HAVING SUM(oi.price * oi.quantity) > 500
ORDER BY total_spent DESC;
"""

parser = SQLParser()
try:
    ast = parser.parse(sql)
    print("Parsing successful!")
except Exception as e:
    print(f"Parsing failed: {e}")
