from struct import Struct
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, FloatType,StringType, BooleanType, ArrayType, DateType

redisServerSchema = StructType([
    StructField("key",StringType()),
    StructField("existType",StringType()),
    StructField("Ch",BooleanType()),
    StructField("Incr",BooleanType()),
    StructField("zSetEntries",ArrayType(
        StructType([
            StructField("element",StringType()),
            StructField("Score",StringType())
        ])
    ))
])

customerJSONSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email",StringType()),
    StructField("phone",StringType()),
    StructField("birthYear", StringType())
    # StructField("birthDay",DateType())
])

stediEventsSchema = StructType([
    StructField("customer",StringType()),
    StructField("score",FloatType()),
    # StructField("score",StringType()),
    StructField("riskDate",DateType())
])


spark = SparkSession.builder.appName("RedisKafkaStream").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets","earliest") \
    .load()

redisServerStreamingDF = redisServerRawStreamingDF \
    .selectExpr("CAST(value AS STRING)")

redisServerStreamingDF.withColumn("value", from_json("value",redisServerSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("RedisSortedSet")

spark.sql("SELECT key, zSetEntries[0].element as encodedCustomer \
          FROM RedisSortedSet")

customerJSONDF = spark.sql("SELECT CAST(unbase64(encodedCustomer) AS STRING \
                           as customerJSON from RedisSortedSet)")

customerJSONDF.withColumn("customer", from_json("customerJSON",customerJSONSchema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("CustomerRecords")


emailAndBirthDayStreamingDF = spark.sql("SELECT email, brithDay FROM CustomerRecords \
                        WHERE email IS NOT NULL AND birthDay IS NOT NULL")


emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF \
    .withColumn("birthYear", split(emailAndBirthDayStreamingDF['birthDay'],'-')[0])


emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select('email','birthYear')

stediEventsRawStreamingDf = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()


stediEventsStreamingDF = stediEventsRawStreamingDf.selectExpr("CAST(value AS STRING)")


stediEventsStreamingDF.withColumn("value", from_json("value", stediEventsSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("CustomerRisk")

customerRiskStreamingDF = spark.sql("SELECT customer, score FROM CustomerRisk")

joinedDF = customerRiskStreamingDF.join(emailAndBirthYearStreamingDF, \
        customerRiskStreamingDF.customer == emailAndBirthYearStreamingDF.email)

query = joinedDF \
    .selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic","stedi-graph") \
    .start() \
    .awaitTermination()

