from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField,DoubleType, StructType, StringType, BooleanType, ArrayType, DateType

spark = SparkSession.builder.appName("CustomerRisk").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

rawStreamDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","localhost:9092") \
        .option("subscribe", "stedi-events") \
        .option("startingOffsets", "earliest") \
        .load()


stringStreamDF = rawStreamDF.selectExpr("CAST(value AS STRING)")

jsonSchema = StructType([
    StructField("customer", StringType()),
    StructField("score", DoubleType()),
    StructField("riskSate",DateType())
])

jsonStreamDF = stringStreamDF.withColumn("value",from_json("value",jsonSchema))
jsonStreamDF.select(
        col('value.customer').alias('customer'),
        col('value.score').alias('score'),
        col('value.riskDate').alias('riskDate')
    ).createOrReplaceTempView('CustomerRisk')

customerRiskStreamingDF = spark.sql(
        "SELECT customer, score \
        FROM CustomerRisk"
    )

query = customerRiskStreamingDF \
    .writeStream \
    .outputMode('append') \
    .format('console') \
    .start() \
    .awaitTermination()
