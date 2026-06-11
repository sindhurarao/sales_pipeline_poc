import uuid
from datetime import datetime
import logging
from helpers.pre_processor import apply_pre_processing
from transformation.transformer import Transformer
from cleanser.rule_driven_cleanser import RuleDrivenCleanser
from validation.rule_validator import RuleValidator
from writers.delta_writer import DeltaWriter
from helpers.audit_helper import AuditLogger
from transformation.join_processor import JoinProcessor

def run(spark, config):
    run_id = str(uuid.uuid4())
    start_time = datetime.now()
    auditor = AuditLogger(spark, config["audit"]["table"])

    logger = logging.getLogger("auto_load_to_bronze")
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s"
        )
    logger.info(f"Run Id       : {run_id}")

    target_writer = DeltaWriter(config["target"]["table"],config.get("write_options",{}))
    quarantine_writer = DeltaWriter(config["validation"]["quarantine_table"],{})

    if config.get("pre_processing") is not None:
        logger.info(f"Pre-processing as per {config.get("pre_processing")}")
        bronze_df = apply_pre_processing(read_source(spark, config), config)
    else:
        bronze_df = read_source(spark, config)

    if config.get("joins") is not None:
        logger.info(f"Joining as per {config.get("joins")}")
        joined_df = JoinProcessor.apply( bronze_df, config.get("joins", []))
    else:
        joined_df = bronze_df

    if config.get("transformations") is not None:
        logger.info(f"Transforming as per {config.get("transformations")}")
        transformed_df = Transformer().apply(joined_df,config.get("transformations", []))
    else:
        transformed_df = joined_df

    if config["validation"]["enabled"]:
        rules_df = spark.table(config["validation"]["rules_table"])
        cleansed_df = RuleDrivenCleanser().apply(transformed_df, rules_df)
        silver_df, quarantine_df = RuleValidator().apply(cleansed_df, rules_df)

        if quarantine_df is not None:
            quarantine_writer.write(quarantine_df)

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

    logger.info(f"Silver DF Schema:")
    silver_df.printSchema()

    silver_df.write.format("delta").mode("append").saveAsTable(config['target']['table'])

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