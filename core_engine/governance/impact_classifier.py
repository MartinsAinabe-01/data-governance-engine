# ===========================================================
# GOVERNANCE IMPACT CLASSIFIER
#
# Converts compatibility + field drift signals
# into enterprise impact tiers (1–5)
#
# Tier Model:
#   TIER 1 – Breaking Schema Change (Block Deploy)
#   TIER 2 – Type Change (Manual Review Required)
#   TIER 3 – Required Flag Change
#   TIER 4 – Additive Compatible Change
#   TIER 5 – Metadata / No Impact
# ===========================================================


def classify_impact(decision_label, comparison_result, field_drift):

    impact_tier = 5
    drift_category = "NONE"
    requires_review = False
    blocks_deployment = False

    # -------------------------------------------------------
    # TIER 1 – Breaking Field Removal or Major Version Break
    # -------------------------------------------------------
    if decision_label == "FIELD_BREAKING_CHANGE":
        impact_tier = 1
        drift_category = "BREAKING_SCHEMA_CHANGE"
        requires_review = True
        blocks_deployment = True

    elif comparison_result == "MAJOR_UPGRADE":
        impact_tier = 1
        drift_category = "MAJOR_VERSION_BREAK"
        requires_review = True
        blocks_deployment = True

    # -------------------------------------------------------
    # TIER 2 – Type Changes
    # -------------------------------------------------------
    elif field_drift and field_drift["type_changes"]:
        impact_tier = 2
        drift_category = "TYPE_CHANGE"
        requires_review = True
        blocks_deployment = False

    # -------------------------------------------------------
    # TIER 3 – Required Flag Changes
    # -------------------------------------------------------
    elif field_drift and field_drift["required_changes"]:
        impact_tier = 3
        drift_category = "REQUIRED_FLAG_CHANGE"
        requires_review = True
        blocks_deployment = False

    # -------------------------------------------------------
    # TIER 4 – Additive Compatible Changes
    # -------------------------------------------------------
    elif field_drift and field_drift["added_fields"]:
        impact_tier = 4
        drift_category = "ADDITIVE_CHANGE"
        requires_review = False
        blocks_deployment = False

    # -------------------------------------------------------
    # TIER 5 – Exact Match
    # -------------------------------------------------------
    else:
        impact_tier = 5
        drift_category = "NO_IMPACT"
        requires_review = False
        blocks_deployment = False

    return {
        "impact_tier": impact_tier,
        "drift_category": drift_category,
        "requires_review": requires_review,
        "blocks_deployment": blocks_deployment
    }