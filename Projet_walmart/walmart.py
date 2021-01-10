### Exercice ###

## 1) Start a simple Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_number
from pyspark.sql.functions import max, min
from pyspark.sql.functions import mean
from pyspark.sql.functions import corr
from pyspark.sql.functions import year
from pyspark.sql.functions import month


spark = SparkSession.builder.appName('walmart_stock').getOrCreate()
sc = spark.sparkContext # Lancement de la session Spark


## 2) Load the Walmart Stock CSV File
df = spark.read.option("header", True).csv(r"C:\Users\Hosti\Desktop\walmart_stock.csv")
df.show(3) # On charge la base de données et on lance les trois premières lignes pour être sûre que tout fonctionne


## 3) What are the column names ?
print("Les noms des colonnes sont :", df.columns) # On cherche les noms de colonnes


## 4) What does the Schema look like?
print("\n ")
df.printSchema()


## 5) Create a new dataframe with a column called HV_Ratio that is the ratio of the High Priceversus volume of stock traded for a day
df_ratio = df.withColumn('HV_Ratio', df['High']/df['Volume']).select(['HV_Ratio'])
df_ratio.show() # Création d'un DataFrame contenant uniquement une colonne représentant le ratio


## 6) What day had the Peak High in Price?
print(u"Le jour avec le prix le plus élevé est le :", df.orderBy(df['High'].desc()).select(['Date']).head(1)[0]['Date'])


## 7) What is the mean of the Close column?
print("\n")
print(df.select(mean('Close')).show())


## 8) What is the max and min of the Volume column?
print("\nWhat is the max and min of the Volume column?")
print(df.select(max('Volume'),min('Volume')).show())


## 9) How many days was the Close lower than 60 dollars?
df.filter(df['Close'] < 60).count()


## 10) What percentage of the time was the High greater than 80 dollars ?
df.filter('High > 80').count() * 100/df.count()


## 11) What is the max High per year?

# Voir le code SQL



## 12) What is the average Close for each Calendar Month?

# Voir le code SQL




