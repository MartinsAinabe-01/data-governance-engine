# ===========================================================
# DAY 14 – HYBRID COMPATIBILITY ENGINE (ARCHITECTURE ALIGNED)
#
# Architectural Layers:
# [A1] CLI Interface Contract
# [A2] Infrastructure Validation
# [A3] Policy Load
# [A4] Contract Load
# [A5] Version Comparison + Compatibility Enforcement
# [A6] Data Processing
# [A7] Observability Summary
# [A8] Governance Enforcement
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
# VERSION COMPARATOR
# -----------------------------------------------------------
def compare_versions(contract_version, expected_version):

    try:
        exp_major, exp_minor = map(int, expected_version.split("."))
        con_major, con_minor = map(int, contract_version.split("."))
    except Exception:
        raise ValueError("Invalid version format. Expected MAJOR.MINOR")

    if con_major != exp_major:
        if con_major > exp_major:
            return "MAJOR_UPGRADE"
        return "MAJOR_DOWNGRADE"

    if con_minor > exp_minor:
        return "MINOR_UPGRADE"

    if con_minor < exp_minor:
        return "MINOR_DOWNGRADE"

    return "EQUAL"


# -----------------------------------------------------------
# COMPATIBILITY POLICY ENGINE
# -----------------------------------------------------------
def evaluate_compatibility(comparison_result, mode):

    if mode == "strict":
        return comparison_result == "EQUAL"

    if mode == "forward_minor":
        return comparison_result in ["EQUAL", "MINOR_UPGRADE"]

    if mode == "backward_minor":
        return comparison_result in ["EQUAL", "MINOR_DOWNGRADE"]

    if mode == "hybrid":
        return comparison_result in [
            "EQUAL",
            "MINOR_UPGRADE",
            "MINOR_DOWNGRADE"
        ]

    raise ValueError(f"Unknown compatibility mode: {mode}")


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

        for row in reader:
            total_rows += 1

            try:
                city, spend = validate_row_against_contract(
                    row, contract, violation_counter
                )

                if city not in city_data:
                    city_data[city] = {"total": 0, "count": 0}

                city_data[city]["total"] += spend
                city_data[city]["count"] += 1

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

    # [A1] CLI
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file")
    parser.add_argument("output_file")
    parser.add_argument("--reject-file", default="rejects.csv")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--contract", default="contract_v1.json")
    parser.add_argument("--policy", default="compatibility_policy.json")

    args = parser.parse_args()

    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)

    # [A2] Infrastructure Validation
    if not os.path.exists(args.input_file):
        terminate_pipeline(logger, "Input file does not exist", 1)

    if not os.path.exists(args.contract):
        terminate_pipeline(logger, "Contract file does not exist", 1)

    if not os.path.exists(args.policy):
        terminate_pipeline(logger, "Policy file does not exist", 1)

    logger.info("===================================")
    logger.info("PIPELINE EXECUTION START")

    start_time = time.time()

    # [A3] Load Policy
    with open(args.policy, "r") as f:
        policy = json.load(f)

    expected_version = policy.get("expected_version")
    compatibility_mode = policy.get("compatibility_mode")

    logger.info(f"Expected Version: {expected_version}")
    logger.info(f"Compatibility Mode: {compatibility_mode}")

    # [A4] Load Contract
    with open(args.contract, "r") as f:
        contract = json.load(f)

    contract_version = contract.get("version")
    logger.info(f"Loaded Contract Version: {contract_version}")

    # [A5] Version Comparison + Compatibility Enforcement
    comparison_result = compare_versions(
        contract_version,
        expected_version
    )

    logger.info(f"Version Comparison Result: {comparison_result}")

    is_allowed = evaluate_compatibility(
        comparison_result,
        compatibility_mode
    )

    if not is_allowed:
        terminate_pipeline(
            logger,
            f"COMPATIBILITY FAILURE: Mode '{compatibility_mode}' "
            f"does not allow '{comparison_result}'",
            2
        )

    # [A6] Data Processing
    total, valid, rejected, violations = calculate_city_average(
        args.input_file,
        args.output_file,
        args.reject_file,
        contract
    )

    # [A7] Observability
    duration = round(time.time() - start_time, 4)

    logger.info("---------- RUN SUMMARY ----------")
    logger.info(f"Total Rows : {total}")
    logger.info(f"Valid Rows : {valid}")
    logger.info(f"Rejected   : {rejected}")
    logger.info(f"Duration   : {duration}")

    logger.info("PIPELINE EXECUTION END")
    logger.info("===================================")

    raise SystemExit(0)