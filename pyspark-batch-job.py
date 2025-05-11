from __future__ import print_function

import sys
import numpy as np
import pandas as pd
from time import sleep
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    print("***** Current Logged In User: ******")
    print(spark.sparkContext.sparkUser())
    print("PySpark version: {}".format(spark.version))
    # Create a pandas Series
    pser = pd.Series([1, 3, 5, np.nan, 6, 8]) 
    print("the pandas series")
    print(pser)
    print("Sleeping for 5sec before Stopping Session")
    sleep(5)
    spark.stop()
