def compare_contract_fields(expected_contract, actual_contract):
    """
    Detects field-level differences between two contracts.

    Returns:
        {
            "added_fields": [],
            "removed_fields": [],
            "type_changes": [],
            "required_changes": []
        }
    """

    expected_fields = expected_contract.get("fields", {})
    actual_fields = actual_contract.get("fields", {})

    expected_set = set(expected_fields.keys())
    actual_set = set(actual_fields.keys())

    added_fields = list(actual_set - expected_set)
    removed_fields = list(expected_set - actual_set)

    type_changes = []
    required_changes = []

    for field in expected_set & actual_set:

        expected_type = expected_fields[field].get("type")
        actual_type = actual_fields[field].get("type")

        if expected_type != actual_type:
            type_changes.append(field)

        expected_required = expected_fields[field].get("required")
        actual_required = actual_fields[field].get("required")

        if expected_required != actual_required:
            required_changes.append(field)

    return {
        "added_fields": added_fields,
        "removed_fields": removed_fields,
        "type_changes": type_changes,
        "required_changes": required_changes
    }