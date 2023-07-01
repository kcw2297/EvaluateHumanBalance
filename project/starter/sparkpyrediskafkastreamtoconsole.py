from struct import Struct
from xmlrpc.client import Boolean
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType


redisServerSchema = StructType([
    StructField("key", StringType()),
    StructField("existType",StringType()),
    StructField("Ch", BooleanType()),
    StructField("Incr",BooleanType()),
    StructField("zSetEntries",ArrayType(
        StructType([
            StructField("element",StringType()),
            StructField("Score",StringType())
        ])
    ))
])



customerSchmea = StructType([
    # StructField("customerName",StringType()),
    StructField("customer",StringType()),
    # StructField("phone",StringType()),
    StructField("score", StringType()),
    StructField("email",StringType()),
    StructField("birthDay",StringType())
])

spark = SparkSession.builder.appName("RedisKafkaStream").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

rawStreamDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()


stringStreamDF = rawStreamDF.selectExpr("CAST(value AS STRING)")

jsonStreamDF = stringStreamDF.withColumn("value",from_json("value", redisServerSchema))
jsonStreamDF.select(
    col('value.key').alias('key'),
    col('value.existType').alias('existType'),
    col('value.Ch').alias('ch'),
    col('value.Incr').alias('incr'),
    col('value.zSetEntries').alias('zSetEntries')
).createOrReplaceTempView("RedisSortedSet")


encodedCustomerDF = spark.sql(
    "SELECT zSetEntries[0].element as encodedCustomer\
    FROM RedisSortedSet"
    )

decodedCustomerDF = encodedCustomerDF.withColumn(
    "customer",
    unbase64(encodedCustomerDF.encodedCustomer).cast("string")
    )

decodedCustomerDF.withColumn("customer",
        from_json("customer", customerSchmea)) \
        .select(col("customer.*")) \
        .createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark.sql(
    "SELECT customer.email, customer.birthDay \
    FROM CustomerRecords \
    WHERE customer.email IS NOT NULL AND customer.birthDay IS NOT NULL"
)

emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF \
    .withColumn('birthYear', split(col("birthDay"),"-").getItem(0))

query = emailAndBirthYearStreamingDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()