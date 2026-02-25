# ===========================================================
# DAY 14 – POLICY-DRIVEN VERSION ENFORCEMENT
#
# Evolution:
#   - Centralized termination handler
#   - Version validation driven by JSON policy
#   - No direct exit() calls
#   - Explicit audit-safe messaging
#
# Architectural Layers:
# [A1] CLI Interface Contract
# [A2] Infrastructure Validation
# [A3] Policy Load
# [A4] Contract Load + Version Validation
# [A5] Data Processing Layer
# [A6] Observability Layer
# [A7] Governance Enforcement
# ===========================================================

import json
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
# GOVERNANCE TERMINATION HANDLER
# -----------------------------------------------------------
def terminate_pipeline(logger, reason_message, exit_code, rows_started=False):

    logger.error(reason_message)

    if not rows_started:
        logger.error("PIPELINE TERMINATED BEFORE DATA PROCESSING")
        logger.error("NO ROWS WERE PROCESSED")

    logger.error(f"GOVERNANCE EXIT CODE: {exit_code}")
    logger.info("===================================")

    raise SystemExit(exit_code)


# -----------------------------------------------------------
# VERSION VALIDATION LAYER
# -----------------------------------------------------------
def validate_contract_version(contract_version, expected_version):

    if not contract_version or not expected_version:
        raise ValueError("Version values must not be None.")

    try:
        expected_major, expected_minor = expected_version.split(".")
        contract_major, contract_minor = contract_version.split(".")
    except Exception:
        raise ValueError("Invalid version format. Expected MAJOR.MINOR")

    if contract_major != expected_major:
        return {"status": "MAJOR_MISMATCH"}

    if int(contract_minor) < int(expected_minor):
        return {"status": "MINOR_TOO_OLD"}

    return {"status": "PASS"}


# -----------------------------------------------------------
# DATA PROCESSING LAYER
# -----------------------------------------------------------
def validate_row_against_contract(row, contract, violation_counter):

    fields = contract.get("fields", {})

    for field_name, rules in fields.items():

        value = row.get(field_name)

        if rules.get("required") and (value is None or value == ""):
            violation_counter[field_name] = violation_counter.get(field_name, 0) + 1
            raise ValueError(f"Missing required field: {field_name}")

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


def validate_schema_against_contract(csv_fields, contract):

    contract_fields = set(contract.get("fields", {}).keys())
    csv_fields = set(csv_fields)

    return contract_fields - csv_fields, csv_fields - contract_fields


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
            logger.warning(f"Extra fields detected (not in contract): {extra_fields}")

        for row in reader:
            total_rows += 1

            try:
                city, spend = validate_row_against_contract(
                    row, contract, violation_counter
                )
                process_row(city_data, city, spend)
                valid_rows += 1

            except Exception as e:
                rejected_rows += 1
                logger.error(f"Rejected row: {row} | Reason: {e}")
                rejects.append({
                    "original_row": str(row),
                    "error_reason": str(e)
                })

    # Write output
    with open(output_file, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["city", "average_spend"])
        writer.writeheader()
        for city, data in city_data.items():
            avg = data["total"] / data["count"]
            writer.writerow({"city": city, "average_spend": round(avg, 2)})

    if rejects:
        with open(reject_file, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=["original_row", "error_reason"])
            writer.writeheader()
            writer.writerows(rejects)

    return total_rows, valid_rows, rejected_rows, violation_counter


# -----------------------------------------------------------
# EXECUTION ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="City Spend Aggregation Pipeline (Batch Mode)"
    )

    parser.add_argument("input_file")
    parser.add_argument("output_file")
    parser.add_argument("--reject-file", default="rejects.csv")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--max-reject-rate", type=float, default=None)
    parser.add_argument("--contract", default="contract_v1.json")
    parser.add_argument("--policy", default="compatibility_policy.json")

    args = parser.parse_args()

    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)

    # [A2] Infrastructure Validation
    if not os.path.exists(args.input_file):
        terminate_pipeline(logger, f"Input file does not exist: {args.input_file}", 1)

    if not os.path.exists(args.contract):
        terminate_pipeline(logger, f"Contract file does not exist: {args.contract}", 1)

    if not os.path.exists(args.policy):
        terminate_pipeline(logger, f"Policy file does not exist: {args.policy}", 1)

    logger.info("===================================")
    logger.info("PIPELINE EXECUTION START")

    start_time = time.time()

    # [A3] Load Policy FIRST
    with open(args.policy, "r") as f:
        policy = json.load(f)

    expected_version = policy.get("expected_version")
    compatibility_mode = policy.get("compatibility_mode")

    logger.info("Loaded Compatibility Policy")
    logger.info(f"Expected Version: {expected_version}")
    logger.info(f"Compatibility Mode: {compatibility_mode}")

    # [A4] Load Contract
    with open(args.contract, "r") as f:
        contract = json.load(f)

    contract_version = contract.get("version")
    logger.info(f"Loaded Contract Version: {contract_version}")

    version_result = validate_contract_version(
        contract_version,
        expected_version
    )

    if version_result["status"] == "MAJOR_MISMATCH":
        terminate_pipeline(
            logger,
            f"Major version mismatch. Expected {expected_version}, but received {contract_version}",
            2
        )

    if version_result["status"] == "MINOR_TOO_OLD":
        terminate_pipeline(
            logger,
            f"Contract minor version too old. Expected >= {expected_version}, but received {contract_version}",
            2
        )

    # [A5] Run Pipeline
    try:
        total, valid, rejected, violation_counter = calculate_city_average(
            args.input_file,
            args.output_file,
            args.reject_file,
            contract
        )
    except Exception as e:
        terminate_pipeline(
            logger,
            f"CONTRACT SCHEMA FAILURE: {e}",
            2
        )

    duration = round(time.time() - start_time, 4)

    # [A6] Summary Logging
    logger.info("---------- RUN SUMMARY ----------")
    logger.info(f"Total Rows Processed : {total}")
    logger.info(f"Valid Rows           : {valid}")
    logger.info(f"Rejected Rows        : {rejected}")
    logger.info(f"Duration (seconds)   : {duration}")

    # [A7] Governance Enforcement
    if args.max_reject_rate is not None and total > 0:
        reject_rate = rejected / total
        if reject_rate > args.max_reject_rate:
            terminate_pipeline(
                logger,
                "CONTRACT BREACH: Reject rate exceeded threshold",
                2,
                rows_started=True
            )

    logger.info("PIPELINE EXECUTION END")
    logger.info("===================================")

    raise SystemExit(0)