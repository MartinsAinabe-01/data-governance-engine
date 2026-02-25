import csv

with open("employees.csv", newline="") as file:
	reader = csv.DictReader(file)

	for row in reader:
		salary = int(row["salary"])

		if salary >= 110000:
			print(row["name"], "is high earner")
		if salary < 110000:
			print(row["name"], "is lower earner")