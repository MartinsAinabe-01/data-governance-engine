import csv

input_file = "customers.csv"
output_file = "calgary_high_spenders.csv"

with open(input_file, mode="r") as file:
    reader = csv.DictReader(file)
    results = []

    for row in reader:
        age = int(row["age"])
        spend = int(row["spend"])
        city = row["city"]

        if city == "Calgary" and spend > 1000:
            row["spend_category"] = "High"
            results.append(row)

with open(output_file, mode="w", newline="") as file:
    fieldnames = ["id", "name", "age", "city", "spend", "spend_category"]
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(results)

print("ETL Process Complete.")
