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
#Cette fonction permet de simuler un point p avec deux coordonnées x et y pour déterminer si ce dernier est à l’intérieur ou à l'extérieur du cercle. 



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
#Fonction qui crée une dataframe permettant de créer une aléatoire entre -0.5 et 0.5. On récupère ensuite la longueur 

n = 100000
#On initialise le nombre d'essaies de points (qu'ils soient à l'intérieur ou à l'extérieur du cercle)

t_0 = time()
#On enregistre le temps actuel

pi2 = pi_estimator_spark(n)
#Enrigistre la valeur Pi avec l'approche Spark

print("Le temps d'execution pour l'approche Spark est de :",np.round(time()-t_0, 3))
#Affiche le temps d'execution du code pour l'approche Spark

print("Pi est egal a : %f" % pi2)
#Affiche la valeur de Pi pour l'approche Spark

t_0 = time()
#On enregistre le temps actuel

pi = pi_estimator_numpy(n)
#Enrigistre la valeur Pi avec l'approche Numpy

print("Le temps d'execution pour l'approche Numpy est de :",np.round(time()-t_0, 3))
#Affiche le temps d'execution du code pour l'approche Numpy

print("Pi est egal a : %f" % pi)
#Affiche la valeur de Pi pour l'approche Numpy

