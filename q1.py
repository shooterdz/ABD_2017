from __future__ import print_function
import sys
import os
import shutil
from operator import add
from pyspark.sql import SparkSession


def removeId(ligne):
	ligne = [x.encode('UTF-8') for x in ligne]
	del ligne[0]
	return ligne


if __name__ == "__main__":

	if (os.path.exists("./res_q1")):
		shutil.rmtree("./res_q1")
	if len(sys.argv) != 2:
		print("Utilisation: countLocation.py <file>", file=sys.stderr)
		exit(-1)

	spark = SparkSession.builder.appName("Question 1").getOrCreate()

	lignes = spark.read.text(sys.argv[1]).repartition(10).rdd.map(lambda r : r[0])
	lignes = lignes.map(lambda x: x.split(' ')).flatMap(lambda y: removeId(y))
	lignes = lignes.map(lambda x: x.split(',')).map(lambda a: (a[0], int(a[1]))).reduceByKey(lambda a,b:int(int(a) + int(b))).sortBy(lambda a : a[1], ascending = False)


	lignes.saveAsTextFile('res_q1')


	spark.stop()
