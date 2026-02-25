import csv

total = 0

with open("employees.csv", newline="") as file:
	reader = csv.DictReader(file)

	for row in reader:
		total += int(row["salary"])

print("Total payroll:", total)