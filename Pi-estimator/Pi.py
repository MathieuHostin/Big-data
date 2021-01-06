import numpy as np
from random import random
import math
from pyspark import SparkContext, SparkConf
import time
from operator import add
from pyspark.sql import SparkSession
from time import time

spark = SparkSession.builder.appName('CalculatePi').getOrCreate()
sc = spark.sparkContext

def is_point_inside_unit_circle(p):
    x, y = random(),random() 
    if x*x + y*y < 1:
        return 1
    else:
        return 0


def pi_estimator_spark(n):
    count = sc.parallelize(range(0, n, 1)).map(is_point_inside_unit_circle).reduce(add)
    return 4.0 * count / n
    

def pi_estimator_numpy(n):
    data = np.random.uniform(-0.5, 0.5, size=(n, 2))
    inside = len(
        np.argwhere(
            np.linalg.norm(data, axis=1) < 0.5
        )
    )
    return 4*(inside / n)

n = 100000

t_0 = time()
pi2 = pi_estimator_spark(n)
print("Le temps d'execution pour l'approche Spark est de :",np.round(time()-t_0, 3))
print("Pi est egal a : %f" % pi2)

t_0 = time()
pi = pi_estimator_numpy(n)
print("Le temps d'execution pour l'approche Numpy est de :",np.round(time()-t_0, 3))
print("Pi est egal a : %f" % pi)

