import sqlite3
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T

def get_spark_connection():
     spark = SparkSession \
         .builder \
         .config(conf=SparkConf().setAppName("pyspark—to-sqlserver")  # type: ignore
         .set("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs","false") 
         .set("spark.driver.memory", "8g")
         .set("spark.executor.memory", "8g")
         .set("spark.executor.instances", "4")
         .set("spark.executor.cores", '6')) \
         .enableHiveSupport() \
         .getOrCreate()
    
     return spark

# 创建sparksession
spark = get_spark_connection()
spark.sparkContext.setLogLevel("Error")

path1 = 'C:/Users/WZH8SGH/OneDrive - Bosch Group/02-SQL Server QC/check2/order check.csv'
path2 = 'C:/Users/WZH8SGH/OneDrive - Bosch Group/02-SQL Server QC/check2/sql_server.csv'
path3 = 'C:/Users/WZH8SGH/OneDrive - Bosch Group/02-SQL Server QC/check2/extra_orders.xlsx'

df_mart = spark.read.csv(path1,header=True)
df_server = spark.read.csv(path2,header=True)
print(df_mart.columns)
print(df_server.columns)

# diff_amount
diff_amount = df_mart.join(df_server,(df_mart['pk_order_no']==df_server['cCO_number']) 
                        & (df_mart['pk_order_item_no']==df_server['cCO_Item'])
                        & (df_mart['order_type'] == df_server['CORDER_TYPE'])
                        ,how='left') \
                        .select(
                            'pk_order_no'
                            ,'pk_order_item_no'
                            ,'order_type'
                            ,'order_doc_date'
                            ,'is_open_order'
                            ,'sales_org'
                            ,'sold_to_party'
                            ,'distr_channel'
                            ,'item_goods_issue_qty'
                            ,'item_qty'
                            ,'net_value_lc'
                        ) \
                        .where(F.col('cCO_number').isNull())
                        # .where(F.col('net_value_lc').cast(T.DecimalType(18,2)) != F.col('IAMOUNT').cast(T.DecimalType(18,2)))
diff_amount.show()

diff_amount.toPandas().to_excel(path3)