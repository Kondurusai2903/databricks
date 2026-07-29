from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

SOURCE_PATH = 'abfss://project-transportation@dbstorageacc123.dfs.core.windows.net/storage-data/city'

@dp.materialized_view(
    name='transportation.bronze.city',
    comment='City Raw Data Processing',
    table_properties={
        'quality': 'bronze',
        'layer': 'bronze',
        'source_format': 'csv',
        'delta.enableChangeDataFeed': 'true',
        'delta.autoOptimize.optimizeWrite': 'true',
        'delta.autoOptimize.autoCompact': 'true'
    }
)
def city_bronze():
    df = spark.read.format('csv')\
        .option('header', 'true')\
        .option('inferSchema', 'true')\
        .option('mode', 'PERMISSIVE')\
        .option('rescuedDataColumn', '_rescued_data')\
        .load(SOURCE_PATH)

    df = df.withColumn('file_name', col('_metadata.file_path'))\
        .withColumn('ingest_datetime', current_timestamp())

    return df