# ===========================================================
# ARCHITECTURE PIPELINE – BATCH MODE CONTRACT ENFORCEMENT
#
# Execution Hierarchy:
#
# [A1] Parse CLI contract (argparse schema validation)
# [A2] Validate infrastructure (input file existence)
# [A3] Run pipeline (data processing layer)
# [A4] Calculate duration (observability layer)
# [A5] Log summary metrics (operational transparency)
# [A6] Compute reject rate (governance layer)
#      - If breach → exit(2)  [Data Contract Violation]
#      - If no breach → exit(0)  [Success]
#
# Exit Code Semantics:
# 0 → Success
# 1 → System/Infrastructure Failure
# 2 → Data Contract Breach
#
# Architectural Layers Represented:
# - Interface Contract (CLI schema)
# - Infrastructure Validation Layer
# - Data Processing Layer
# - Observability Layer
# - Governance Layer
#
# This file represents Batch Enforcement Mode.
# Streaming mode will alter enforcement behavior.
# ===========================================================

import os
import csv
import time
import logging
import argparse


# -----------------------------------------------------------
# LOGGING CONFIGURATION
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
# DATA PROCESSING LAYER
# -----------------------------------------------------------
def validate_row(row):
    city = row["city"]
    spend = int(row["spend"])

    if not city:
        raise ValueError("Missing city")

    return city, spend


def process_row(city_data, city, spend):

    if city not in city_data:
        city_data[city] = {"total": 0, "count": 0}

    city_data[city]["total"] += spend
    city_data[city]["count"] += 1


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


def write_rejects(rejects, reject_file):

    if not rejects:
        return

    with open(reject_file, mode="w", newline="") as file:
        fieldnames = ["original_row", "error_reason"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(rejects)


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

    # -------------------------------------------------------
    # [A1] Parse CLI Contract
    # -------------------------------------------------------
    parser = argparse.ArgumentParser(
        description="City Spend Aggregation Pipeline (Batch Mode)"
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

    parser.add_argument(
        "--max-reject-rate",
        type=float,
        default=None,
        help="Maximum allowed reject rate (e.g. 0.2 for 20%)."
    )

    args = parser.parse_args()

    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)

    # -------------------------------------------------------
    # [A2] Validate Infrastructure
    # -------------------------------------------------------
    if not os.path.exists(args.input_file):
        logger.error(f"Input file does not exist: {args.input_file}")
        exit(1)

    start_time = time.time()

    logger.info("===================================")
    logger.info("PIPELINE EXECUTION START")
    logger.info(f"Input File  : {args.input_file}")
    logger.info(f"Output File : {args.output_file}")
    logger.info(f"Reject File : {args.reject_file}")

    # -------------------------------------------------------
    # [A3] Run Pipeline
    # -------------------------------------------------------
    total, valid, rejected = calculate_city_average(
        args.input_file,
        args.output_file,
        args.reject_file
    )

    # -------------------------------------------------------
    # [A4] Calculate Duration
    # -------------------------------------------------------
    duration = round(time.time() - start_time, 4)

    # -------------------------------------------------------
    # [A5] Log Summary Metrics
    # -------------------------------------------------------
    logger.info("---------- RUN SUMMARY ----------")
    logger.info(f"Total Rows Processed : {total}")
    logger.info(f"Valid Rows           : {valid}")
    logger.info(f"Rejected Rows        : {rejected}")
    logger.info(f"Duration (seconds)   : {duration}")
    logger.info("---------------------------------")

    # -------------------------------------------------------
    # [A6] Governance Layer – Reject Threshold Enforcement
    # -------------------------------------------------------
    if args.max_reject_rate is not None and total > 0:

        reject_rate = rejected / total
        logger.info(f"Reject Rate          : {round(reject_rate, 4)}")

        if reject_rate > args.max_reject_rate:
            logger.error("CONTRACT BREACH: Reject rate exceeded threshold")
            logger.error(f"Threshold            : {args.max_reject_rate}")
            logger.error(f"Actual Reject Rate   : {round(reject_rate, 4)}")
            logger.error("Exiting with code 2 (Data Contract Violation)")
            exit(2)

    logger.info("PIPELINE EXECUTION END")
    logger.info("===================================")

    exit(0)

# -----------------------------------------------------------
# USAGE (CLI Contract)
#
# Basic Run:
# python day11_pipeline.py customers.csv output.csv
#
# With Reject Threshold:
# python day11_pipeline.py customers.csv output.csv \
#     --max-reject-rate 0.2
#
# With Custom Reject File + Debug Logging:
# python day11_pipeline.py customers.csv output.csv \
#     --reject-file bad_rows.csv \
#     --log-level DEBUG \
#     --max-reject-rate 0.2
#
# CLI parameters represent the Interface Contract Layer.
# -----------------------------------------------------------