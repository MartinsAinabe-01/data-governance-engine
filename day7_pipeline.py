import csv

import time
from datetime import datetime


# -----------------------------------------------------------
# FUNCTION: validate_row
# PURPOSE:
#   Validate and extract clean data from row
# -----------------------------------------------------------
def validate_row(row):
    city = row["city"]
    spend = int(row["spend"])

    if not city:
        raise ValueError("Missing city")

    return city, spend


# -----------------------------------------------------------
# FUNCTION: process_row
# PURPOSE:
#   Update aggregation state
# -----------------------------------------------------------
def process_row(city_data, city, spend):

    if city not in city_data:
        city_data[city] = {"total": 0, "count": 0}

    city_data[city]["total"] += spend
    city_data[city]["count"] += 1


# -----------------------------------------------------------
# FUNCTION: write_output
# PURPOSE:
#   Write aggregation results to CSV
# -----------------------------------------------------------
def write_output(output_file, city_data):

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


# -----------------------------------------------------------
# FUNCTION: write_rejects
# PURPOSE:
#   Persist rejected rows
# -----------------------------------------------------------
def write_rejects(rejects):

    if not rejects:
        return

    with open("rejects.csv", mode="w", newline="") as file:
        fieldnames = ["original_row", "error_reason"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(rejects)


# -----------------------------------------------------------
# FUNCTION: calculate_city_average
# PURPOSE:
#   Main processing function
# -----------------------------------------------------------
def calculate_city_average(input_file, output_file):

    city_data = {}
    rejects = []

    total_rows = 0
    valid_rows = 0
    rejected_rows = 0

    with open(input_file, mode="r") as file:
        reader = csv.DictReader(file)

        for row in reader:
            total_rows += 1

            try:
                city, spend = validate_row(row)
                process_row(city_data, city, spend)
                valid_rows += 1

            except Exception as e:
                rejected_rows += 1
                rejects.append({
                    "original_row": str(row),
                    "error_reason": str(e)
                })

    write_output(output_file, city_data)
    write_rejects(rejects)

    return total_rows, valid_rows, rejected_rows


# -----------------------------------------------------------
# EXECUTION ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":

    # -----------------------------------
    # PIPELINE START
    # -----------------------------------
    start_time = time.time()
    start_timestamp = datetime.now()

    print("\n===================================")
    print("PIPELINE EXECUTION START")
    print("Start Time:", start_timestamp)
    print("===================================\n")

    total, valid, rejected = calculate_city_average(
        "customers.csv",
        "city_average.csv"
    )

    # -----------------------------------
    # PIPELINE END
    # -----------------------------------
    end_time = time.time()
    end_timestamp = datetime.now()
    duration = round(end_time - start_time, 4)

    print("\n---------- RUN SUMMARY ----------")
    print(f"Total Rows Processed : {total}")
    print(f"Valid Rows           : {valid}")
    print(f"Rejected Rows        : {rejected}")
    print("---------------------------------\n")

    print("===================================")
    print("PIPELINE EXECUTION END")
    print("End Time:", end_timestamp)
    print("Duration (seconds):", duration)
    print("===================================\n")