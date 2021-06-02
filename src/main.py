import sys
import logging
from datetime import datetime

# Create logging object
logger = logging.getLogger('FastFD-logger')
logger.setLevel(logging.DEBUG)

# Create log file
fh = logging.FileHandler(datetime.now().strftime('./logs/log_%Y_%m_%d_%H_%M.log'))

# Log to file and to stdout
sh = logging.StreamHandler(sys.stdout)

# Format
formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)
sh.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(sh)

# Init spark
import findspark

findspark.init()
findspark.find()

from pyspark.sql import SparkSession

from FD import FD
from FastFD import FastFD

spark = SparkSession.builder\
    .master("local")\
    .appName("Distributed-FastFD")\
    .config('spark.ui.port', '4050')\
    .getOrCreate()

dataset = spark.read.csv('./dataset/paper_data.csv', header=True)
fastfd = FastFD(dataset, debug=True, logger=logger)

hard_FD : list = fastfd.execute()
