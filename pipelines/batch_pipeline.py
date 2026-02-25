# ===========================================================
# BATCH PIPELINE – ENTERPRISE COMPATIBILITY ENGINE
#
# Layers:
# [1] CLI Interface
# [2] Infrastructure Validation
# [3] Policy Load
# [4] Baseline Contract Load
# [5] Active Contract Load
# [6] Version Comparison
# [7] Policy Evaluation
# [8] Field Drift Detection
# [9] Decision Classification
# [10] Severity Classification
# [11] Governance Artifact
# [12] Profile-Based Enforcement
# ===========================================================

import json
import os
import time
import logging
import argparse

from core_engine.versioning.comparator import compare_versions
from core_engine.versioning.schema_diff import compare_contract_fields
from core_engine.compatibility.policy_engine import evaluate_compatibility
from core_engine.governance.termination import terminate_pipeline
from core_engine.governance.audit_writer import write_compatibility_report
from core_engine.governance.impact_classifier import classify_impact
from core_engine.governance.cicd_gate import evaluate_cicd_gate


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
        handlers=[logging.StreamHandler()]
    )


# -----------------------------------------------------------
# EXECUTION ENTRY POINT
# -----------------------------------------------------------
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("input_file")
    parser.add_argument("output_file")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--contract", default="contracts/contract_v2.json")
    parser.add_argument("--policy", default="policies/compatibility_policy.json")

    args = parser.parse_args()

    configure_logging(args.log_level)
    logger = logging.getLogger(__name__)

    logger.info("===================================")
    logger.info("PIPELINE EXECUTION START")

    # -----------------------------------------------------------
    # [2] Infrastructure Validation
    # -----------------------------------------------------------
    if not os.path.exists(args.contract):
        terminate_pipeline(logger, "Active contract file not found", 1)

    if not os.path.exists(args.policy):
        terminate_pipeline(logger, "Policy file not found", 1)

    # -----------------------------------------------------------
    # [3] Load Policy
    # -----------------------------------------------------------
    with open(args.policy, "r") as f:
        policy = json.load(f)

    expected_version = policy.get("expected_version")
    compatibility_mode = policy.get("compatibility_mode")
    execution_profile = policy.get("execution_profile", "batch")

    logger.info(f"Expected Version: {expected_version}")
    logger.info(f"Compatibility Mode: {compatibility_mode}")
    logger.info(f"Execution Profile: {execution_profile}")

    # -----------------------------------------------------------
    # [4] Load Baseline Contract (Registry Simulation)
    # -----------------------------------------------------------
    baseline_contract_path = f"contracts/contract_v{expected_version.split('.')[0]}.json"

    if not os.path.exists(baseline_contract_path):
        logger.warning("Baseline contract not found. Field comparison skipped.")
        baseline_contract = None
    else:
        with open(baseline_contract_path, "r") as f:
            baseline_contract = json.load(f)

    # -----------------------------------------------------------
    # [5] Load Active Contract
    # -----------------------------------------------------------
    with open(args.contract, "r") as f:
        contract = json.load(f)

    contract_version = contract.get("version")
    logger.info(f"Contract Version: {contract_version}")

    # -----------------------------------------------------------
    # [6] Version Comparison
    # -----------------------------------------------------------
    comparison_result = compare_versions(
        contract_version,
        expected_version
    )

    logger.info(f"Version Comparison Result: {comparison_result}")

    # -----------------------------------------------------------
    # [7] Policy Evaluation
    # -----------------------------------------------------------
    is_allowed = evaluate_compatibility(
        comparison_result,
        compatibility_mode
    )

    # -----------------------------------------------------------
    # [8] Field-Level Drift Detection
    # -----------------------------------------------------------
    if baseline_contract:
        field_drift = compare_contract_fields(
            baseline_contract,
            contract
        )
    else:
        field_drift = None

    # -----------------------------------------------------------
    # [9] Decision Classification (Version-Based)
    # -----------------------------------------------------------
    if not is_allowed:
        decision_label = "HARD_FAIL"

    elif comparison_result == "EXACT_MATCH":
        decision_label = "PASS"

    elif compatibility_mode == "override":
        decision_label = "SOFT_PASS_DRIFT"

    else:
        decision_label = "PASS_WITH_FORWARD_COMPAT"

    # -----------------------------------------------------------
    # [9b] Breaking Field Override (Dominates Version Logic)
    # -----------------------------------------------------------
    breaking_changes = False

    if field_drift:
        if (
            field_drift["removed_fields"]
            or field_drift["type_changes"]
            or field_drift["required_changes"]
        ):
            breaking_changes = True

    if breaking_changes:
        decision_label = "FIELD_BREAKING_CHANGE"
        is_allowed = False  # Breaking change overrides policy

    # -----------------------------------------------------------
    # [10] Severity Classification
    # -----------------------------------------------------------
    if decision_label in ["FIELD_BREAKING_CHANGE", "HARD_FAIL"]:
        severity = "CRITICAL"
        drift_detected = True

    elif decision_label == "SOFT_PASS_DRIFT":
        severity = "WARNING"
        drift_detected = True

    elif decision_label == "PASS_WITH_FORWARD_COMPAT":
        severity = "INFO"
        drift_detected = True

    else:  # EXACT MATCH
        severity = "INFO"
        drift_detected = False

    # -----------------------------------------------------------
    # [10b] Governance Impact Classification
    # -----------------------------------------------------------
    impact_result = classify_impact(
        decision_label,
        comparison_result,
        field_drift
    )

    # -----------------------------------------------------------
    # [10c] CI/CD Gate Evaluation
    # -----------------------------------------------------------
    gate_result = evaluate_cicd_gate(
        impact_result["impact_tier"]
    )
    # -----------------------------------------------------------
    # [11] Governance Artifact
    # -----------------------------------------------------------
    compatibility_decision = {
        "timestamp": time.time(),
        "expected_version": expected_version,
        "contract_version": contract_version,
        "comparison_result": comparison_result,
        "compatibility_mode": compatibility_mode,
        "execution_profile": execution_profile,
        "allowed": is_allowed,
        "decision": decision_label,
        "severity": severity,
        "drift_detected": drift_detected,
        "field_drift": field_drift,
        "impact_tier": impact_result["impact_tier"],
        "drift_category": impact_result["drift_category"],
        "requires_review": impact_result["requires_review"],
        "blocks_deployment": impact_result["blocks_deployment"],
        "cicd_gate": gate_result
    }

    logger.info(f"Compatibility Decision: {compatibility_decision}")

    report_path = write_compatibility_report(compatibility_decision)
    logger.info(f"Compatibility Report Written: {report_path}")

    # -----------------------------------------------------------
    # [11b] CI/CD Enforcement Simulation
    # -----------------------------------------------------------
    if gate_result["blocks_pipeline"]:
        logger.error("CI/CD Gate Blocked Deployment.")

    # -----------------------------------------------------------
    # [12] Profile-Based Enforcement
    # -----------------------------------------------------------
    if not is_allowed:

        if execution_profile == "batch":
            terminate_pipeline(
                logger,
                "Compatibility enforcement failed (batch mode).",
                2
            )

        elif execution_profile == "streaming":
            logger.warning(
                "Streaming profile detected. Compatibility drift logged but pipeline continues."
            )

        else:
            terminate_pipeline(
                logger,
                f"Unknown execution profile: {execution_profile}",
                2
            )

    logger.info("PIPELINE EXECUTION END")
    logger.info("===================================")