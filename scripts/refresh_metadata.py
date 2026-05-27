"""Cron entry point for refreshing runtime sensor metadata.

All logic lives in :mod:`sendai_pipeline.refresh`; this script is a thin
shim so logger names resolve under the ``sendai_pipeline`` package and
:func:`sendai_pipeline.logging_setup.configure_logging` covers them.
"""

import sys

from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.refresh import main

if __name__ == "__main__":
    load_dotenv(find_dotenv(usecwd=True))
    sys.exit(main(sys.argv[1:]))
