def evaluate_compatibility(comparison_result, mode):
    """
    Evaluates version comparison result against compatibility mode.

    comparison_result:
        EXACT_MATCH
        MINOR_UPGRADE
        MAJOR_UPGRADE
        MINOR_DOWNGRADE
        MAJOR_DOWNGRADE

    mode:
        strict
        forward_minor
        override
    """

    if mode == "strict":
        return comparison_result == "EXACT_MATCH"

    elif mode == "forward_minor":
        return comparison_result in ["EXACT_MATCH", "MINOR_UPGRADE"]

    elif mode == "override":
        # Soft fail mode
        # Always allow processing
        return True

    else:
        raise ValueError(f"Unknown compatibility mode: {mode}")