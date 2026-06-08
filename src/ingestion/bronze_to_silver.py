import uuid
from datetime import datetime
import logging
from helpers.pre_processor import apply_pre_processing
from transformation.transformer import Transformer
from cleanser.rule_driven_cleanser import RuleDrivenCleanser
from validation.rule_validator import RuleValidator
from writers.writer_adapter import WriterAdapter
from helpers.audit_helper import AuditLogger

def run(self, spark, config):
    run_id = str(uuid.uuid4())
    start_time = datetime.now()

    transformer=Transformer(),
    cleanser=RuleDrivenCleanser(),
    validator=RuleValidator(),
    writer=WriterAdapter(self.spark),
    auditor=AuditLogger(self.spark,self.config["audit"]["table"])

    logger = logging.getLogger("auto_load_to_bronze")
    if not logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(message)s"
        )
    logger.info(f"Run Id       : {run_id}")

    bronze_df = apply_pre_processing(self.read_source(), self.config, self.spark)
    joined_df = self.join_processor.apply(
        bronze_df,
        self.config.get("joins", [])
    )
    transformed_df = transformer.apply(
        joined_df,
        self.config.get("transformations", [])
        )

    if self.config["validation"]["enabled"]:
        rules_df = self.spark.table(self.config["validation"]["rules_table"])
        cleansed_df = cleanser.apply(transformed_df, rules_df)
        silver_df, quarantine_df = validator.apply(cleansed_df, rules_df)

        writer.write_table(
            quarantine_df,
            self.config["validation"]["quarantine_table"],
            mode="append"
        )

        auditor.log(
            run_id=f"{run_id}",
            source=self.config["source"]["table"],
            target=self.config["validation"]["quarantine_table"],
            status="SUCCESS",
            start_time=start_time,
            end_time=datetime.now(),
            records_ingested=quarantine_df.count()
        )
        logger.info(f"Successfully written {quarantine_df.count()} records to "
                    f"{self.config["validation"]["quarantine_table"]}")

    else:
        silver_df = transformed_df

    writer.write(
        silver_df,
        self.config["target"],
        self.config["write_options"]
    )

    logger.info(f"Successfully written {silver_df.count()} records to "
                f"{self.config["target"]}")
    auditor.log(
        run_id=f"{run_id}",
        source=self.config["source"]["table"],
        target=self.config["target"],
        status="SUCCESS",
        start_time=start_time,
        end_time=datetime.now(),
        records_ingested=silver_df.count()
    )


def read_source(self):
    ingestion_type = self.config["ingestion_type"]
    if ingestion_type == "table":
        return self.spark.table(self.config["source"]["table"])
    if ingestion_type == "path":
        reader = self.spark.read
        for key, value in self.config["source"].get("options", {}).items():
            reader = reader.option(key, value)
        return reader.format(
            self.config["source"]["format"]
        ).load(self.config["source"]["path"])
    raise ValueError(f"Unsupported ingestion_type: {ingestion_type}")