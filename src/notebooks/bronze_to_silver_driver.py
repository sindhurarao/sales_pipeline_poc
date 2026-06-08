import json
from ingestion.bronze_to_silver import run

dbutils.widgets.text("config", "")
config = json.loads(dbutils.widgets.get("config"))

run(spark,config)

