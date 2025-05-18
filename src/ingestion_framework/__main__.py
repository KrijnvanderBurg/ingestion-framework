"""
TODO
"""

import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import Any

from ingestion_framework.job import JobAbstract, JobContext
from ingestion_framework.utils.file_handler import FileHandler, FileHandlerContext
from ingestion_framework.utils.log_handler import set_logger

logger: logging.Logger = set_logger(__name__)


def main() -> None:
    """TODO"""
    logger.info("Starting something...")

    parser = ArgumentParser(description="config driven etl.")
    parser.add_argument("--filepath", required=True, type=str, help="confeti filepath")
    args = parser.parse_args()
    logger.info("args: %s", args)

    if args.filepath == "":
        raise ValueError("Filepath is required.")
    filepath: Path = Path(args.filepath)

    file_handler: FileHandler = FileHandlerContext.factory(filepath=str(filepath))
    dict_: dict[str, Any] = file_handler.read()

    job = JobContext.from_confeti(confeti=dict_)
    job_cls: JobAbstract = job.from_confeti(confeti=dict_)
    job_cls.execute()

    logger.info("Exiting.")


if __name__ == "__main__":
    main()
