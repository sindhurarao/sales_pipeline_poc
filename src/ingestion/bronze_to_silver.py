import uuid
from datetime import datetime
import logging
from helpers.pre_processor import apply_pre_processing
from transformation.transformer import Transformer
from cleanser.rule_driven_cleanser import RuleDrivenCleanser
from validation.rule_validator import RuleValidator
from writers.writer_adapter import WriterAdapter
from helpers.audit_helper import AuditLogger
from transformation.join_processor import JoinProcessor

def run(spark, config):
    run_id = str(uuid.uuid4())
    start_time = datetime.now()

    transformer = Transformer()
    cleanser = RuleDrivenCleanser()
    validator = RuleValidator()
    writer = WriterAdapter(spark)
    auditor = AuditLogger(spark, config["audit"]["table"])

    logger = logging.getLogger("auto_load_to_bronze")
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s"
        )
    logger.info(f"Run Id       : {run_id}")

    bronze_df = apply_pre_processing(read_source(spark, config), config)
    joined_df = JoinProcessor.apply(
        bronze_df,
        config.get("joins", [])
    )

    transformed_df = transformer.apply(validated_df,config.get("transformations", []))

    if config["validation"]["enabled"]:
        rules_df = spark.table(config["validation"]["rules_table"])
        cleansed_df = cleanser.apply(transformed_df, rules_df)
        silver_df, quarantine_df = validator.apply(cleansed_df, rules_df)

        if quarantine_df is not None:
            writer.write_table(
                quarantine_df,
                config["validation"]["quarantine_table"],
                mode="append"
            )

            auditor.log(
                run_id=f"{run_id}",
                source=config["source"]["table"],
                target=config["validation"]["quarantine_table"],
                status="SUCCESS",
                start_time=start_time,
                end_time=datetime.now(),
                records_ingested=quarantine_df.count()
            )
            logger.info(f"Successfully written {quarantine_df.count()} records to "
                        f"{config['validation']['quarantine_table']}")

    else:
        silver_df = transformed_df

    writer.write(
        silver_df,
        config["target"],
        config["write_options"]
    )

    logger.info(f"Successfully written {silver_df.count()} records to "
                f"{config['target']}")
    auditor.log(
        run_id=f"{run_id}",
        source=config["source"]["table"],
        target=config["target"],
        status="SUCCESS",
        start_time=start_time,
        end_time=datetime.now(),
        records_ingested=silver_df.count()
    )


def read_source(spark, config):
    ingestion_type = config["ingestion_type"]
    if ingestion_type == "table":
        return spark.table(config["source"]["table"])
    if ingestion_type == "path":
        reader = spark.read
        for key, value in config["source"].get("options", {}).items():
            reader = reader.option(key, value)
        return reader.format(
            config["source"]["format"]
        ).load(config["source"]["path"])
    raise ValueError(f"Unsupported ingestion_type: {ingestion_type}")