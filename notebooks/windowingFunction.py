from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Initialize SparkSession
def create_spark_session():
    spark = SparkSession.builder \
        .appName('Windowing Functions') \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# Function to create DataFrame
def createDataFrame(spark):
    testdata = [("John", 25, "Engineer"), ("Eve", 32, "Engineer"),("Rowan", 24, "Engineer"), ("Aravinda", 26, "Engineer")]
    df = spark.createDataFrame(testdata, ["Name", "Age", "Profession"])
    return df

# Function to define window specification partitioned by "Profession" and ordered by "Age"
def windowPartitionByProfessionAndOrderByAge():
    window_spec = Window.partitionBy("Profession").orderBy("Age")
    return window_spec

# Function to add rank column using window function
def addRankToDf(df, window_spec):
    df_with_rank = df.withColumn("Rank", rank().over(window_spec))
    return df_with_rank

# Function to display DataFrame with rank
def displayDataFrame(df):
    df.show()

# Main function
def main():
    
    # Create Spark session
    spark = create_spark_session()

    # Create DataFrame
    df = createDataFrame(spark)

    # Define window specification
    window_spec = windowPartitionByProfessionAndOrderByAge()

    # Add rank column
    df_with_rank = addRankToDf(df, window_spec)

    # Display DataFrame with rank
    displayDataFrame(df_with_rank)


if __name__ == "__main__":
    main()
