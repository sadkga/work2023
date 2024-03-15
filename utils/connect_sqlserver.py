
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext

spark = SparkSession.builder.config(conf=SparkConf().setAppName("pyspark—to-sqlserver")
                                    .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false").set("spark.driver.memory", "4g").set("spark.executor.memory", "4g")).enableHiveSupport().getOrCreate()
# spark = SparkSession \
#         .builder \
#         .appName("pyspark—to-sqlserver") \
#         .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs","false") \
#         .getOrCreate()


jdbc_url = "jdbc:sqlserver://192.169.0.4:1433;DatabaseName=apac-data-mart-01"

connection_properties = {
    "user": "waz8sgh@sql-server-01",
    "password": "Bosch2023"
}


df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dass.dws_wks_prch_order_smy_df") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .options(**connection_properties) \
    .load()


df.show()

# df.write \
#     .format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("dbtable", "dass.dim_wks_vin_level_id") \
#     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
#     .options(**connection_properties) \
#     .mode("overwrite") \
#     .save()
