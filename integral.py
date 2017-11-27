from __future__ import print_function

import sys
import numpy as np
from random import random
from operator import add

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("Integration")\
        .getOrCreate()

    n = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    a = 1.0
    b = 10.0
    partitions = 10
    pas = ((b-a) / n)

    def f(i):
	i = a + i * pas
        x = 1 / i
        return x

    count = spark.sparkContext.parallelize(range(0, n), partitions).map(f).reduce(add)
    count = count * pas
    print("\nLa valeur du count est : ", count, "\n")

    spark.stop()
