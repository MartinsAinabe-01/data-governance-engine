# -----------------------------------------------------------
# VERSION COMPARATOR MODULE
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