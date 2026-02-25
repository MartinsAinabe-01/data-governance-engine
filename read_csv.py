import csv

with open("employees.csv", newline="") as file:
	reader = csv.DictReader(file)

	for row in reader:
		print(row["name"], "earns", row["salary"])


