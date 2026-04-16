import csv
import random

first_names = ["John", "Jane", "Michael", "Sarah", "James", "Linda", "Robert", "Jennifer", "William", "Elizabeth", "David", "Susan", "Joseph", "Jessica", "Charles", "Karen", "Christopher", "Nancy", "Matthew", "Margaret", "Daniel", "Lisa", "Paul", "Betty", "Kevin", "Dorothy", "Brian", "Sandra", "Edward", "Ashley", "Ronald", "Kimberly", "Anthony", "Donna", "Mark", "Emily", "Joshua", "Cynthia", "Steven", "Angela", "Andrew", "Melissa", "Ryan", "Stephanie", "Jacob", "Katherine", "Gary", "Amy", "Nicholas", "Christine"]
last_names = ["Smith", "Johnson", "Brown", "Davis", "Wilson", "Martinez", "Taylor", "Anderson", "Thomas", "Garcia", "Rodriguez", "Miller", "White", "Harris", "Martin", "Lee", "Thompson", "Jackson", "Moore", "Lewis", "Walker", "Hall", "Young", "Hernandez", "Wright", "King", "Baker", "Hill", "Lopez", "Scott", "Green", "Adams", "Baker", "Gonzalez", "Nelson", "Carter", "Mitchell", "Perez", "Roberts", "Turner", "Phillips", "Campbell", "Parker", "Evans", "Edwards", "Collins", "Stewart", "Morris", "Rogers", "Reed"]

with open("showcase_users.csv", "w", newline="", encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name", "age"])
    for i in range(1, 10001):
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        age = random.randint(18, 75)
        writer.writerow([i, name, age])

print("Generated 1,000 realistic users in showcase_users.csv")
