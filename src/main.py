from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import concat
from pyspark.sql.functions import lit
from pyspark.sql.functions import col
from pyspark.sql.functions import lead
from pyspark.sql.functions import when
from pyspark.sql.functions import sum



'''
HELPER FUNCTIONS
'''

# Initialize a spark session.
def init_spark():
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    return spark

#Useful functions to print RDDs and Dataframes.
def toCSVLineRDD(rdd):
    '''
    This function convert an RDD or a DataFrame into a CSV string
    '''
    a = rdd.map(lambda row: ",".join([str(elt) for elt in row]))\
           .reduce(lambda x, y: '\n'.join([x,y]))
    return a + '\n'

def toCSVLine(data):
    '''
    Convert an RDD or a DataFrame into a CSV string
    '''
    if isinstance(data, RDD):
        return toCSVLineRDD(data)
    elif isinstance(data, DataFrame):
        return toCSVLineRDD(data.rdd)
    return None



'''
Pyspark Dataframe

'''

def calculate_session_duration(filename):
    spark = init_spark()

    df = spark.read.csv(filename, header=True, inferSchema=True)

    # Create 'userId' by concatenating 'fullVisitorID' and 'visitNumber', separated by '-'
    df = df.withColumn("userId", concat(col("fullVisitorID"), lit("-"), col("visitNumber")))

    # Define window spec for ordering by time within each user session
    windowSpec = Window.partitionBy("userId").orderBy("time")

    # Calculate the difference in time for consecutive hits to get the pageview duration
    df = df.withColumn("next_hit_time", lead("time", 1).over(windowSpec))
    df = df.withColumn("pageview_duration", when(col("next_hit_time").isNull(), 0)
                       .otherwise(col("next_hit_time") - col("time")))

    # Filter for product detail views (where eCommerceAction.action_type == "2")
    prod_view_df = df.filter(df['action_type'] == '2')

    # Aggregate session durations by userId and productSKU, summing up durations to get total session duration
    aggregate_web_stats = prod_view_df.groupBy("userId", "productSKU") \
        .agg(sum("pageview_duration").alias("session_duration"))

    # Optionally rename columns to match desired output
    aggregate_web_stats = aggregate_web_stats.withColumnRenamed("productSKU", "itemId")

    # Show the result
    aggregate_web_stats.show(10)

    aggregate_web_stats.write.csv('../data/data_v1.csv', header=True, mode='overwrite')



calculate_session_duration("../data/raw data.csv")