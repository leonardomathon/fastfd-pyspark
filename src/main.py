import findspark

findspark.init()
findspark.find()

from pyspark.sql import SparkSession

from FastFD import FastFD

spark = SparkSession.builder\
    .master("local")\
    .appName("Milestone1")\
    .config('spark.ui.port', '4050')\
    .getOrCreate()

dataset = spark.read.csv('./dataset/paper_data.csv', header=True)
fastfd = FastFD(dataset, debug=True)

hard_FD : list = fastfd.execute()

