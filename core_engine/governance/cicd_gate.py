# ===========================================================
# CI/CD DEPLOYMENT GATE ENGINE
#
# Converts Governance Impact Tier into
# simulated CI/CD merge/deploy decisions.
#
# This layer represents:
# - GitHub PR checks
# - Azure DevOps approvals
# - GitLab pipeline gates
#
# ===========================================================


def evaluate_cicd_gate(impact_tier):

    gate_status = "ALLOW"
    requires_manual_approval = False
    blocks_pipeline = False
    action_required = None

    # -------------------------------------------------------
    # Tier 1 – Block Deployment
    # -------------------------------------------------------
    if impact_tier == 1:
        gate_status = "BLOCK"
        blocks_pipeline = True
        requires_manual_approval = True
        action_required = "Schema Breaking Change – Deployment Blocked"

    # -------------------------------------------------------
    # Tier 2 – Manual Review Required
    # -------------------------------------------------------
    elif impact_tier == 2:
        gate_status = "REVIEW_REQUIRED"
        requires_manual_approval = True
        blocks_pipeline = False
        action_required = "Type Change – Manual Architecture Review Required"

    # -------------------------------------------------------
    # Tier 3 – Warning Approval
    # -------------------------------------------------------
    elif impact_tier == 3:
        gate_status = "WARNING"
        requires_manual_approval = True
        blocks_pipeline = False
        action_required = "Required Flag Change – Review Recommended"

    # -------------------------------------------------------
    # Tier 4 – Auto Approve with Logging
    # -------------------------------------------------------
    elif impact_tier == 4:
        gate_status = "AUTO_APPROVE"
        requires_manual_approval = False
        blocks_pipeline = False
        action_required = "Additive Compatible Change – Auto Approved"

    # -------------------------------------------------------
    # Tier 5 – No Impact
    # -------------------------------------------------------
    else:
        gate_status = "NO_ACTION"
        requires_manual_approval = False
        blocks_pipeline = False
        action_required = "No Compatibility Impact"

    return {
        "gate_status": gate_status,
        "requires_manual_approval": requires_manual_approval,
        "blocks_pipeline": blocks_pipeline,
        "action_required": action_required
    }