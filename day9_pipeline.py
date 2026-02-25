import os
import csv
import time
import logging
import sys
from datetime import datetime




# -----------------------------------------------------------
# LOGGING CONFIGURATION
# PURPOSE:
#   Structured logging to console + file
# -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


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
def write_rejects(rejects, reject_file):

    if not rejects:
        return

    with open(reject_file, mode="w", newline="") as file:
        fieldnames = ["original_row", "error_reason"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(rejects)


# -----------------------------------------------------------
# FUNCTION: calculate_city_average
# PURPOSE:
#   Main processing function
# -----------------------------------------------------------
def calculate_city_average(input_file, output_file, reject_file):

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
                logger.error(f"Rejected row: {row} | Reason: {e}")
                rejects.append({
                    "original_row": str(row),
                    "error_reason": str(e)
                })

    write_output(output_file, city_data)
    write_rejects(rejects, reject_file)

    return total_rows, valid_rows, rejected_rows


# -----------------------------------------------------------
# EXECUTION ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":

    # -----------------------------------
    # ARGUMENT VALIDATION
    # -----------------------------------
    if len(sys.argv) < 3:
        logger.error("Usage: python day9_pipeline.py <input_file> <output_file> [reject_file]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    if len(sys.argv) == 4:
        reject_file = sys.argv[3]
    else:
        reject_file = "rejects.csv"

    # -----------------------------------
    # FILE VALIDATION
    # -----------------------------------
    if not os.path.exists(input_file):
        logger.error(f"Input file does not exist: {input_file}")
        sys.exit(1)

    start_time = time.time()

    logger.info("===================================")
    logger.info("PIPELINE EXECUTION START")
    logger.info(f"Input File  : {input_file}")
    logger.info(f"Output File : {output_file}")
    logger.info(f"Reject File : {reject_file}")

    total, valid, rejected = calculate_city_average(
        input_file,
        output_file,
        reject_file
    )

    duration = round(time.time() - start_time, 4)

    logger.info("---------- RUN SUMMARY ----------")
    logger.info(f"Total Rows Processed : {total}")
    logger.info(f"Valid Rows           : {valid}")
    logger.info(f"Rejected Rows        : {rejected}")
    logger.info("---------------------------------")

    logger.info(f"Duration (seconds)   : {duration}")
    logger.info("PIPELINE EXECUTION END")
    logger.info("===================================")

    sys.exit(0)