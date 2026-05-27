"""Production pipeline package for Sendai FIWARE integration."""

import logging

# Library code never configures handlers; the NullHandler ensures that
# ``import sendai_pipeline`` does not emit warnings or write to the root
# logger when no entry point has called ``configure_logging`` yet.
logging.getLogger(__name__).addHandler(logging.NullHandler())
