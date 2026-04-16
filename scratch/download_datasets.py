import urllib.request
import csv
import os

os.makedirs("benchmarks/data", exist_ok=True)

print("Downloading Titanic dataset...")
titanic_url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
urllib.request.urlretrieve(titanic_url, "benchmarks/data/titanic_raw.csv")

# Clean Titanic
with open("benchmarks/data/titanic_raw.csv", 'r', encoding='utf-8') as f_in, open("benchmarks/data/titanic.csv", 'w', encoding='utf-8', newline='') as f_out:
    reader = csv.DictReader(f_in)
    writer = csv.DictWriter(f_out, fieldnames=reader.fieldnames)
    writer.writeheader()
    for row in reader:
        row['Name'] = row['Name'].replace(',', '')
        if not row['Age']: row['Age'] = '0'
        if not row['Fare']: row['Fare'] = '0.0'
        writer.writerow(row)
os.remove("benchmarks/data/titanic_raw.csv")
print("Saved titanic.csv")

print("Downloading Pokemon dataset...")
pokemon_url = "https://gist.githubusercontent.com/armgilles/194bcff35001e7eb53a2a8b441e8b2c6/raw/92200bc0a673d5ce2110aaad4544ed6c4010f687/pokemon.csv"
urllib.request.urlretrieve(pokemon_url, "benchmarks/data/pokemon_raw.csv")

# Clean Pokemon
with open("benchmarks/data/pokemon_raw.csv", 'r', encoding='utf-8') as f_in, open("benchmarks/data/pokemon.csv", 'w', encoding='utf-8', newline='') as f_out:
    reader = csv.DictReader(f_in)
    new_fieldnames = [c.replace(' ', '_').replace('.', '').lower() for c in reader.fieldnames]
    new_fieldnames[new_fieldnames.index('#')] = 'id'
    
    writer = csv.DictWriter(f_out, fieldnames=new_fieldnames)
    writer.writeheader()
    for row in reader:
        new_row = {}
        for old_col, new_col in zip(reader.fieldnames, new_fieldnames):
            val = row[old_col]
            if new_col == 'legendary':
                val = '1' if val == 'True' else '0'
            new_row[new_col] = val
        writer.writerow(new_row)
os.remove("benchmarks/data/pokemon_raw.csv")
print("Saved pokemon.csv")

print("Datasets downloaded successfully!")
