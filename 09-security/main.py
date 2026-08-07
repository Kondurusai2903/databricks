# import spark

# from pyspark.sql.functions import *

spark.sql("""
          CREATE CATALOG IF NOT EXISTS security
           MANAGED LOCATION 'abfss://localpycharm@dbstorageacc123.dfs.core.windows.net'
          """)


spark.sql("""
          CREATE SCHEMA IF NOT EXISTS security.bronze
""")
