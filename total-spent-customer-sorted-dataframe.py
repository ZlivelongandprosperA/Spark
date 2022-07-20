from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

# create schemawhen reading customer-orders
customerOrderSchema = StructType([ \
                     StructField("cust_id", IntegerType(), True), \
                     StructField("item_id", IntegerType(), True), \
                     StructField("amount_spent", FloatType(), True)])

# // Read the file as dataframe
customerDF = spark.read.schema(customerOrderSchema).csv("file:///SparkCourse/customer-orders.csv" )


# Select only customerID and amount spent, we are using agg bcs we 
# are working on aggregated dataset created by groupBy
totalByCustomer = customerDF.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2).alias("total_spent"))

# sort data
totalByCustomerSorted = totalByCustomer.sort("total_spent")

# with totalByCustomerSorted.count() we display all results, not only 20 first
totalByCustomerSorted.show(totalByCustomerSorted.count())

    
spark.stop()

                                                  