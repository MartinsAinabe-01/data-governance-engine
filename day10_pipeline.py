import os
import csv
import time
import logging
import argparse


# -----------------------------------------------------------
# FUNCTION: configure_logging
# PURPOSE:
#   Configure logging level dynamically
# -----------------------------------------------------------
def configure_logging(log_level):

    numeric_level = getattr(logging, log_level.upper(), None)

    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler("pipeline.log"),
            logging.StreamHandler()
        ]
    )


# -----------------------------------------------------------
# FUNCTION: validate_row
# -----------------------------------------------------------
def validate_row(row):
    city = row["city"]
    spend = int(row["spend"])

    if not city:
        raise ValueError("Missing city")

    return city, spend


# -----------------------------------------------------------
# FUNCTION: process_row
# -----------------------------------------------------------
def process_row(city_data, city, spend):

    if city not in city_data:
        city_data[city] = {"total": 0, "count": 0}

    city_data[city]["total"] += spend
    city_data[city]["count"] += 1


# -----------------------------------------------------------
# FUNCTION: write_output
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
# -----------------------------------------------------------
def calculate_city_average(input_file, output_file, reject_file):

    logger = logging.getLogger(__name__)

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

    parser = argparse.ArgumentParser(
        description="City Spend Aggregation Pipeline"
    )

    parser.add_argument("input_file", help="Path to input CSV file")
    parser.add_argument("output_file", help="Path to output CSV file")

    parser.add_argument(
        "--reject-file",
        default="rejects.csv",
        help="Path to reject file (default: rejects.csv)"
    )

    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)"
    )

    args = parser.parse_args()

    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)

    if not os.path.exists(args.input_file):
        logger.error(f"Input file does not exist: {args.input_file}")
        exit(1)

    start_time = time.time()

    logger.info("===================================")
    logger.info("PIPELINE EXECUTION START")
    logger.info(f"Input File  : {args.input_file}")
    logger.info(f"Output File : {args.output_file}")
    logger.info(f"Reject File : {args.reject_file}")

    total, valid, rejected = calculate_city_average(
        args.input_file,
        args.output_file,
        args.reject_file
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

    exit(0)