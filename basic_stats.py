
# coding: utf-8


from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql import SQLContext
#sc.stop()
sc =SparkContext()
sqlContext = SQLContext(sc)
import pyspark.sql.functions as func
import sys
from pyspark.sql.functions import countDistinct



input_data = sys.argv[1]

# reading data 
if input_data[-3:] == 'csv':
    df = sqlContext         .read.format("com.databricks.spark.csv")         .option("header", "true")         .option("inferschema", "true")         .option("mode", "DROPMALFORMED")         .load(input_data) 
elif input_data[-3:] == 'txt':
    df =sc.textFile(input_data)
else:
    df = sqlContext.read.parquet(input_data)

#Statistics

print "Data schema :",df.columns,"\n"
print "Data Types :",[(c.name,c.dataType) for c in df.schema.fields]
print "\n"
print "Total records in the data :",df.count(),"\n"
print "Distinct records :", df.distinct().count(),"\n"
print "\n","Missing/Null values in columns"
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).show()

print "Distinct count for each columns:"
df.select( [ countDistinct(cn).alias("{0}".format(cn)) for cn in df.columns ] ).show()

print "Statistics of Non-string columns:"
for cn in df.schema.fields:
    if str(cn.dataType) != "StringType":
        df.describe([cn.name.format(cn.name)]).show()
    
print "Columns that may could be categorized and their distribution","\n"

for cn in df.columns:
    if df.select(cn.format(cn)).distinct().count() < 30:
        data = df.groupby(cn).agg(func.count("*").alias('cnt'))
        total = data.select("cnt").agg({"cnt": "sum"}).collect().pop()['sum(cnt)']
        data = data.withColumn("percentage", (data['cnt']/total) * 100)
        data.sort(col('percentage').desc()).show()
 

