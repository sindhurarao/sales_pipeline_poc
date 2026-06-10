import sys
sys.path.append("/Workspace/Users/sindhurarao06@gmail.com/sales_pipeline_poc/src")

import json
from ingestion.bronze_to_silver import run

dbutils.widgets.text("config", "")
config = json.loads(dbutils.widgets.get("config"))

run(spark,config)

