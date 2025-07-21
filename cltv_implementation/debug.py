import logging

def debug_parameters(parameters: dict) -> None:
    """
    A debugging node to print the parameters dictionary loaded by Kedro.
    """
    log = logging.getLogger(__name__)
    log.info(f"DEBUG: Parameters loaded by Kedro: {parameters}")
    # This node doesn't return anything, it's just for inspection.
    # If you need it to pass something to another node, you'd return a value.
    pass