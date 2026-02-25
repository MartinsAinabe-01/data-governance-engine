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