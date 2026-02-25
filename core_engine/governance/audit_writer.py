# -----------------------------------------------------------
# GOVERNANCE AUDIT WRITER
# -----------------------------------------------------------

import json
import os
from datetime import datetime


def write_compatibility_report(report_data, report_directory="reports"):

    if not os.path.exists(report_directory):
        os.makedirs(report_directory)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    filename = f"compatibility_report_{timestamp}.json"
    filepath = os.path.join(report_directory, filename)

    with open(filepath, "w") as f:
        json.dump(report_data, f, indent=4)

    return filepath