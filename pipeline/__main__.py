"""`python -m pipeline run --target local` (AD-9). The emr target arrives in story 6."""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from pipeline import spark as spark_mod
from pipeline.config import Conf, ConfigError, load_conf
from pipeline.jobs import bronze, gold, silver
from pipeline.log import configure_logging


def run_local(spark: SparkSession, conf: Conf) -> None:
    bronze.main(spark, conf)
    silver.main(spark, conf)
    gold.main(spark, conf)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m pipeline")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="run bronze, silver and gold")
    run.add_argument("--target", choices=["local"], required=True)
    args = parser.parse_args(argv)

    configure_logging()
    try:
        conf = load_conf()
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.target == "local":
        spark = spark_mod.build_spark("patient-history-local", local=True)
        try:
            run_local(spark, conf)
        finally:
            spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
