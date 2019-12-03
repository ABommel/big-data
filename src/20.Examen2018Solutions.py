# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,../src//py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# # Outils informatiques pour le Big Data
#
# ## Examen du 05 décembre 2018
#
# **Prénom:**
#
# **Nom:**
#
# *Attention vous devez déposer ce fichier sur [cursus](https://cursus.univ-rennes2.fr/course/view.php?id=11467) avant 12h45*
#
# Les données fournies pour cette examen sont les crimes reportés dans une grande ville américaine durant une année.

# +
import os
import findspark

os.environ["JAVA_HOME"]="/Library/Java/JavaVirtualMachines/jdk1.8.0_152.jdk/Contents/Home"
os.environ["SPARK_HOME"]="/usr/local/opt/apache-spark/libexec"
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"

findspark.init()
# -

# ## Exercice 1
#
# Créez une session de type `SparkContext` nommée `sc` avec deux processeurs 

# +
# Create a local spark cluster with 2 workers
from pyspark import SparkContext

sc = SparkContext('local[*]')

# -

# ## Exercise 2
#
# Vérifier que votre session fonctionne correctement en testant le programme suivant:

rdd = sc.parallelize(range(8))  # create collection

rdd.collect()

# ## Exercice 3
#
# Lisez les données se trouvant dans le fichier `big-data/data/philadelphia-crime-data-2015-ytd.csv` et stocker les dans une variable nommée `base_rdd`

base_rdd = sc.textFile("file:///Users/navaro/Desktop/big-data/data/philadelphia-crime-data-2015-ytd.csv")

# **Question**: La variable `base_rdd` contient-elle déjà les données ?
#
#  * [ ] Oui
#  * [X] Non

# Affichons les 10 premières observations:

base_rdd.take(10)

# Filtrons la première ligne pour ne pas tenir compte de la ligne d'entête.

no_header_rdd = base_rdd.filter(lambda line: 'SECTOR' not in line)

no_header_rdd.take(10)

# ## Description des variables
#
# * `DC_DIST` (integer): District number
# * `SECTOR` (integer): Sector or PSA Number
# * `DISPATCH_DATE` (date string): Date of Incident 
# * `DISPATCH_TIME` (time string): Time of Incident
# * `DC_KEY`: (text): Unique ID of each crime
# * `UCR_General` (integer): Rounded Crime Code
# * `TEXT_GENERAL_CODE` (string): Human-readable Crime Code
# * `OBJECTID` (integer): Unique row ID
# * `POINT_X` (decimal): Latitude where crime occurred
# * `POINT_Y` (decimal): Longitude where crime occurred

# ### Exercise 4
#
# Transformer cette variable `base_rdd` pour qu'elle ne contienne que des objets Python
#
# * Décomposer chaque ligne à l'aide de la méthode `split`
# * Creer une nouvelle RDD nommée `data_rdd` contenant une liste d'instances de la classe `CrimeData` qui est un `nametuple`.
#
# Ne prenez en compte que les variables `date_string`, `time_string`, `offense`, `latitude` et `longitude`.

# +
from collections import namedtuple

CrimeData = namedtuple('CrimeData', ['date_string', 'time_string', 'offense', 'latitude', 'longitude'])

def map_line(line):
    cols = line.split(",")
    return CrimeData(date_string=cols[10], 
                   time_string=cols[11], 
                   offense=cols[6], 
                   latitude=cols[7], 
                   longitude=cols[8])
  
data_rdd = no_header_rdd.map(map_line)
# -

print(data_rdd.take(10))

# ### Exercise 5
#
# Grouper les observations par crime et compter les.
#
# Les observations de la variable `offense` présentent quelques anomalies.
# Ecrire le code python permettant de nettoyer les données et stocker le résultat dans une nouvelle rdd nommée `cleaned_data`.
#
# Répondez aux questions suivantes:
#
#  - Combien comptez-vous de meutres durant la période ?
#  
#  Réponse:
#  
#  
#  - Combien de cambriolages de résidence ? 
#  
#  Réponse:
#  
#  
#  - Combien de vols à main armée ? 
#  
#  Réponse:

# +
import re

BAD_OFFENSE_RE = re.compile(r'^\\d+$')

def clean_offense(d):
    d = CrimeData(date_string=d.date_string, 
                    time_string=d.time_string,
                    offense=d.offense.replace('\"', '').strip(),
                    latitude=d.latitude,
                    longitude=d.longitude)
    return d

cleaned_rdd = (data_rdd.map(clean_offense)
               .filter(lambda d: not d.offense.isdigit()))
# -

from operator import itemgetter
offense_counts = cleaned_rdd.map(lambda item: (item.offense, item)).countByKey()
for offense, counts in sorted(offense_counts.items(), key=itemgetter(1), reverse=True):
    print("{0:30s} {1:d}".format(offense, counts))

# ### Exercise 6
#
# Ecrire le code Python permettant de tracer l'histogramme du nombre d'homicides en fonction de l'heure de la journée (0:00-24:00).

# +
from datetime import datetime
time_format = "%H:%M:%S"

def parse_time(s):
    return s.split(':')[0]

def parse_date(s):
    return s.split('-')[1]

result1_rdd = cleaned_rdd.filter(lambda item: item.offense.startswith("Homicide"))\
                         .map(lambda d: (parse_time(d.time_string),1))\
                         .countByKey()

result2_rdd = cleaned_rdd.filter(lambda item: item.offense.startswith("Homicide"))\
                         .map(lambda d: (parse_date(d.date_string),1))\
                         .reduceByKey(lambda a, b: a + b)


results1 = sorted(result1_rdd.items())
results2 = sorted(result2_rdd.collect())

# +
# %matplotlib inline
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

plt.bar([ h[0] for h in results1], [ h[1] for h in results1])                   

# +
# %matplotlib inline
import matplotlib.pyplot as plt
import seaborn as sns
sns.set()

plt.bar([ d[0] for d in results2], [ h[1] for h in results2])      
# -

# # Solution with DataFrame

# +
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp

spark = SparkSession(sc)

df = spark.read.csv("file:///Users/navaro/Desktop/big-data/data/philadelphia-crime-data-2015-ytd.csv",
                   mode="DROPMALFORMED",
                   header=True)

df.show()
# -

df.printSchema()

df.groupBy("TEXT_GENERAL_CODE").count().orderBy("count", ascending=False).show()

# +
from pyspark.sql.functions import when

df.columns
# -

from pyspark.sql.functions import col,udf
from pyspark.sql.types import  StringType
myfunc =  udf(lambda x: x.strip() , StringType())
new_df = df.withColumn("TEXT_GENERAL_CODE", myfunc(col('TEXT_GENERAL_CODE')))
new_df.groupBy("TEXT_GENERAL_CODE").count().orderBy("count", ascending=False).show()

homicides = (new_df.filter("TEXT_GENERAL_CODE == 'Homicide - Criminal'")
.select(hour(new_df["DISPATCH_TIME"])
.alias("hour"))
.groupBy("hour").count().collect())

hours, counts = zip(*[(row.hour,row["count"])  for row in homicides])

# +

plt.bar(hours, counts)
# -


