import csv


# -----------------------------------------------------------
# FUNCTION: calculate_city_average
# PURPOSE:
#   1. Read customer data from input CSV
#   2. Group by city
#   3. Calculate average spend per city
#   4. Write results to output CSV
#   5. Track metrics and rejected rows
# -----------------------------------------------------------
def calculate_city_average(input_file, output_file):

    # -------------------------------
    # STATE CONTAINER
    # -------------------------------
    city_data = {}

    # -------------------------------
    # METRICS COUNTERS
    # -------------------------------
    total_rows = 0
    valid_rows = 0
    rejected_rows = 0

    # -------------------------------
    # REJECT STORAGE
    # -------------------------------
    rejects = []

    # -------------------------------
    # INGESTION + PROCESSING
    # -------------------------------
    with open(input_file, mode="r") as file:
        reader = csv.DictReader(file)

        for row in reader:

            total_rows += 1

            try:
                city = row["city"]
                spend = int(row["spend"])

                if not city:
                    raise ValueError("Missing city")

                # Aggregation logic must be INSIDE try
                if city not in city_data:
                    city_data[city] = {"total": 0, "count": 0}

                city_data[city]["total"] += spend
                city_data[city]["count"] += 1

                valid_rows += 1

            except Exception as e:
                rejected_rows += 1
                rejects.append({
                    "original_row": str(row),
                    "error_reason": str(e)
                })

    # -------------------------------
    # OUTPUT BLOCK
    # -------------------------------
    with open(output_file, mode="w", newline="") as file:
        fieldnames = ["city", "average_spend"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()

        for city, data in city_data.items():
            average = data["total"] / data["count"]

            writer.writerow({
                "city": city,
                "average_spend": round(average, 2)
            })

    # -------------------------------
    # WRITE REJECT FILE
    # -------------------------------
    if rejects:
        with open("rejects.csv", mode="w", newline="") as file:
            fieldnames = ["original_row", "error_reason"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            writer.writeheader()
            writer.writerows(rejects)

    # -------------------------------
    # RUN SUMMARY
    # -------------------------------
    print("---------- RUN SUMMARY ----------")
    print(f"Total Rows Processed: {total_rows}")
    print(f"Valid Rows: {valid_rows}")
    print(f"Rejected Rows: {rejected_rows}")
    print("Output File:", output_file)
    print("Reject File: rejects.csv")


# -----------------------------------------------------------
# EXECUTION ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":
    calculate_city_average("customers.csv", "city_average.csv")