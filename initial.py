# The intention of this script is to implement initial Pyspark functionalities
from pyspark.sql import SparkSession # starting point for a spark session
from pyspark.sql.functions import col, when
from random import randint
from names import get_first_name
from time import time, sleep
import os
import shutil

    
def createNameVector(numberOfPeople : int) -> list:
    '''
    Return a data, which is a list of tuples with the name,age of many people
    numberOfPeople: Size of the generated list
    '''
    left, right = 0, 140
    data = []
    for _ in range(numberOfPeople):
        data.append((get_first_name(), randint(left, right)))

    print(f'Size of data = {len(data)}')

    df = spark.createDataFrame(data, ["Name", "Age"])
    df_filtered = df.filter(col("Age") > 30)
    df_filtered.show()
    if os.path.exists("input.csv"): os.remove("input.csv") # If exists, remove
    df.coalesce(1).write.mode("overwrite").csv("input", header = True)
    
    for file in os.listdir("input"):
        if file.startswith("part-"):
            os.rename(f'input/{file}','input.csv')
    shutil.rmtree("input") 
    return data

def readNameVector():
    df_read = spark.read.csv("input.csv", header = True, inferSchema = True) # Read the csv file
    
    # Remove NAs, create a new columns for demonstration, make a filter, and create a new column based on Age
    df = df_read\
        .dropna() \
        .withColumn("Age * 2", col("Age") * 2) \
        .filter(col("Age") > 18)\
        .withColumn("Legally Adult", when(col("Age") > 18, "Adult").otherwise("Underage"))
    return df # Return the data frame to the user so they can explore it

def createParquet(df):
    df.write.mode("overwrite").parquet("output_parquet") # Write parquet to current folder
    
    df_parquet =  spark.read.parquet("output_parquet") # Read parquet from current folder
    df_parquet.show() # Prints the current df generate from the parquet
    return

if __name__ == "__main__":
    begining = time() # Set the initial time of the application

    # Let's create the initial Spark Session to kickoff the jobs
    spark = SparkSession.builder\
    .appName("Local PySpark Example")\
    .master("local[*]")\
    .getOrCreate()
    
    # Execute the 3 above created functions to simulate the PySpark features
    createNameVector(500)
    df = readNameVector()
    createParquet(df)
    
    end = time() # Get the end time
    duration = end - begining # Calculate the time the application run
    print(f'Duration of operation is = {duration}') # Print it
    print("Executors:", spark.sparkContext._jsc.sc().getExecutorMemoryStatus().keySet()) # Print info
    print("Cluster Manager:", spark.sparkContext.master) # Print info about Cluster Manager
    print("Now visita http://localhost:4040 to checki the details of the execution plan")
    
    sleep(600) # Keep information available for 10 minutes
    spark.stop() # Stop PySpark Session