# ===========================================================
# ARCHITECTURE PIPELINE – BATCH MODE CONTRACT ENFORCEMENT
#
# [A1] Parse CLI
# [A2] Validate Infrastructure
# [A3] Load External Contract
# [A4] Run Pipeline
# [A5] Calculate Duration
# [A6] Log Summary Metrics
# [A7] Governance Layer
# ===========================================================

import json
import os
import csv
import time
import logging
import argparse
EXPECTED_CONTRACT_VERSION = "1.0"


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

def validate_row_against_contract(row, contract, violation_counter):

    fields = contract.get("fields", {})

    for field_name, rules in fields.items():

        value = row.get(field_name)

        # Required validation
        if rules.get("required") and (value is None or value == ""):
            violation_counter[field_name] = violation_counter.get(field_name, 0) + 1
            raise ValueError(f"Missing required field: {field_name}")

        # Type validation
        if value is not None and value != "":
            expected_type = rules.get("type")

            if expected_type == "int":
                try:
                    int(value)
                except:
                    violation_counter[field_name] = violation_counter.get(field_name, 0) + 1
                    raise ValueError(f"Invalid int for field: {field_name}")

            elif expected_type == "string":
                str(value)

    return row["city"], int(row["spend"])


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


def validate_schema_against_contract(csv_fields, contract):

    contract_fields = set(contract.get("fields", {}).keys())
    csv_fields = set(csv_fields)

    missing_fields = contract_fields - csv_fields
    extra_fields = csv_fields - contract_fields

    return missing_fields, extra_fields


def calculate_city_average(input_file, output_file, reject_file, contract):

    logger = logging.getLogger(__name__)

    city_data = {}
    rejects = []
    violation_counter = {}

    total_rows = 0
    valid_rows = 0
    rejected_rows = 0

    with open(input_file, mode="r") as file:
        reader = csv.DictReader(file)

        missing_fields, extra_fields = validate_schema_against_contract(
            reader.fieldnames,
            contract
        )

        if missing_fields:
            raise Exception(f"Schema mismatch - Missing fields: {missing_fields}")

        if extra_fields:
            logging.getLogger(__name__).warning(
                f"Extra fields detected (not in contract): {extra_fields}"
            )

        for row in reader:
            total_rows += 1

            try:
                city, spend = validate_row_against_contract(row, contract, violation_counter)
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

    return total_rows, valid_rows, rejected_rows, violation_counter


# -----------------------------------------------------------
# EXECUTION ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":

    # [A1] Parse CLI
    parser = argparse.ArgumentParser(
        description="City Spend Aggregation Pipeline (Batch Mode)"
    )

    parser.add_argument("input_file")
    parser.add_argument("output_file")

    parser.add_argument("--reject-file", default="rejects.csv")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--max-reject-rate", type=float, default=None,
                        help="Maximum allowed reject rate (e.g. 0.2 for 20%%).")
    parser.add_argument("--contract", default="contract_v1.json")

    args = parser.parse_args()

    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)

    # [A2] Validate Infrastructure
    if not os.path.exists(args.input_file):
        logger.error(f"Input file does not exist: {args.input_file}")
        exit(1)

    if not os.path.exists(args.contract):
        logger.error(f"Contract file does not exist: {args.contract}")
        exit(1)

    start_time = time.time()

    logger.info("===================================")
    logger.info("PIPELINE EXECUTION START")

    # [A3] Load Contract
    with open(args.contract, "r") as f:
        contract = json.load(f)

    logger.info(f"Loaded Contract Version: {contract.get('version')}")

    #if contract.get("version") != EXPECTED_CONTRACT_VERSION:
    contract_version = contract.get("version")
    expected_major, expected_minor = EXPECTED_CONTRACT_VERSION.split(".")
    contract_major, contract_minor = contract_version.split(".")

    if contract_major != expected_major:
        logger.error(
            f"Major version mismatch. Expected {EXPECTED_CONTRACT_VERSION}, "
            f"but received {contract_version}"
        )
        exit(2)

    if int(contract_minor) < int(expected_minor):
        logger.error(
            f"Contract minor version too old. Expected >= {EXPECTED_CONTRACT_VERSION}, "
            f"but received {contract_version}"
        )
        exit(2)
        logger.error(
            f"Contract version mismatch. Expected {EXPECTED_CONTRACT_VERSION}, "
            f"but received {contract.get('version')}"
        )
        exit(2)


    # [A4] Run Pipeline
    try:
        total, valid, rejected, violation_counter = calculate_city_average(
            args.input_file,
            args.output_file,
            args.reject_file,
            contract
        )
    except Exception as e:
        logger.error(f"CONTRACT SCHEMA FAILURE: {e}")
        exit(2)

    # [A5] Calculate Duration
    duration = round(time.time() - start_time, 4)

    # [A6] Log Summary
    logger.info("---------- RUN SUMMARY ----------")
    logger.info(f"Total Rows Processed : {total}")
    logger.info(f"Valid Rows           : {valid}")
    logger.info(f"Rejected Rows        : {rejected}")
    logger.info(f"Duration (seconds)   : {duration}")

    logger.info("---------- CONTRACT VALIDATION SUMMARY ----------")
    logger.info(f"Contract Version     : {contract.get('version')}")
    logger.info(f"Contract Field Count : {len(contract.get('fields', {}))}")

    if violation_counter:
        for field, count in violation_counter.items():
            logger.info(f"{field} -> {count} violations")
    else:
        logger.info("No contract violations detected")

    logger.info("---------------------------------")

    # [A7] Governance Layer
    if args.max_reject_rate is not None and total > 0:
        reject_rate = rejected / total
        if reject_rate > args.max_reject_rate:
            logger.error("CONTRACT BREACH: Reject rate exceeded threshold")
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