# Import necessary PySpark libraries and data types
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType
from pyspark.sql.functions import from_json, col, sum, to_date

# Define the schema for the invoice data. This schema should match the JSON
# format of the messages being produced to the Kafka topic.
# The data types are inferred from the Fact_Invoice.csv file provided.
invoice_schema = StructType([
    StructField("InvoiceID", IntegerType(), True),
    StructField("SellerID", IntegerType(), True),
    StructField("BuyerID", IntegerType(), True),
    StructField("TaxID", IntegerType(), True),
    StructField("DeviceID", IntegerType(), True),
    StructField("Payment_methodID", IntegerType(), True),
    StructField("CancelReasonID", DoubleType(), True),
    StructField("ErrorTypeID", DoubleType(), True),
    StructField("InvoiceTotal_amount", DoubleType(), True),
    StructField("subtotal_amount", DoubleType(), True),
    StructField("tax_amount", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    # Note: Using `to_date` later to handle the date format
    StructField("invoice_Date", StringType(), True),
    StructField("invoice_ModifiedDate", StringType(), True)
])

# Create a SparkSession with the necessary Kafka package.
# The --packages option is crucial for structured streaming to work with Kafka.
# To run this script, you would use:
# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 kafka_invoice_streaming_analysis.py
spark = SparkSession \
    .builder \
    .appName("KafkaInvoiceStreaming") \
    .getOrCreate()

# Set the log level to avoid verbose output
spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka.
# The `subscribe` option specifies the Kafka topic to read from.
# The `kafka.bootstrap.servers` option specifies the Kafka broker(s).
# The `startingOffsets` option is set to 'latest' to start from the most recent
# messages. Change to 'earliest' to read from the beginning of the topic.
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "invoices") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the JSON string from the Kafka message value and apply the defined schema.
# The 'value' column from Kafka is a binary type, so we need to cast it to a string first.
invoice_stream_df = kafka_stream_df.select(
    from_json(col("value").cast("string"), invoice_schema).alias("invoice_data")
).select("invoice_data.*")

# Add a processing column for correct date parsing
invoices_with_dates = invoice_stream_df \
    .withColumn("invoice_Date_formatted", to_date(col("invoice_Date"), "MM-dd-yy"))

# Perform a streaming aggregation to calculate total sales per seller.
# The `groupBy` operation creates a continuous aggregation.
sales_by_seller_stream = invoices_with_dates \
    .groupBy("SellerID") \
    .agg(
        sum("InvoiceTotal_amount").alias("TotalSales"),
        sum("tax_amount").alias("TotalTax")
    )

# Start the streaming query.
# The `format("console")` will write the output to the console.
# The `outputMode("complete")` will output all the rows in the result table
# every time there is an update. Other options are "update" or "append".
# The `trigger` option processes data every 5 seconds.
query = sales_by_seller_stream \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()

# `awaitTermination()` keeps the application running until the query is
# terminated manually or due to a failure.
query.awaitTermination()
