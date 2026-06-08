import json
from ingestion.copy_into_bronze import run as copy_into
from ingestion.load_file_to_bronze import run as load_file
from ingestion.auto_load_to_bronze import run as autoloader

dbutils.widgets.text("config", "")
config = json.loads(dbutils.widgets.get("config"))

ingestion_type = config["ingestion_type"]

if ingestion_type == "copy_into":
    copy_into(spark, config)

elif ingestion_type == "load_file":
    load_file(spark, config)

elif ingestion_type == "auto_load":
    autoloader(spark, config)