import csv

input_file = "customers.csv"
output_file = "city_summary.csv"

city_totals = {}   # This will hold our grouped results

with open(input_file, mode="r") as file:
    reader = csv.DictReader(file)

    for row in reader:
        city = row["city"]
        spend = int(row["spend"])

        # If city not yet in dictionary, initialize it
        if city not in city_totals:
            city_totals[city] = 0

        # Add spend to that city's total
        city_totals[city] += spend

# Now write results to new file
with open(output_file, mode="w", newline="") as file:
    fieldnames = ["city", "total_spend"]
    writer = csv.DictWriter(file, fieldnames=fieldnames)

    writer.writeheader()

    for city, total in city_totals.items():
        writer.writerow({"city": city, "total_spend": total})

print("City summary ETL complete.")
