# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,../src//py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ![Logo](images/apache_spark_logo.png)
#
# - [Apache Spark](https://spark.apache.org) was first released in 2014. 
# - It was originally developed by [Matei Zaharia](http://people.csail.mit.edu/matei) as a class project, and later a PhD dissertation, at University of California, Berkeley.
# - Spark is written in [Scala](https://www.scala-lang.org).
# - All images come from [Databricks](https://databricks.com/product/getting-started-guide).

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# - Apache Spark is a fast and general-purpose cluster computing system. 
# - It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs.
# - Spark can manage "big data" collections with a small set of high-level primitives like `map`, `filter`, `groupby`, and `join`.  With these common patterns we can often handle computations that are more complex than map, but are still structured.
# - It also supports a rich set of higher-level tools including [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) for SQL and structured data processing, [MLlib](https://spark.apache.org/docs/latest/ml-guide.html) for machine learning, [GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html) for graph processing, and Spark Streaming.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Resilient distributed datasets
#
# - The fundamental abstraction of Apache Spark is a read-only, parallel, distributed, fault-tolerent collection called a resilient distributed datasets (RDD).
# - RDDs behave a bit like Python collections (e.g. lists).
# - When working with Apache Spark we iteratively apply functions to every item of these collections in parallel to produce *new* RDDs.
# - The data is distributed across nodes in a cluster of computers.
# - Functions implemented in Spark can work in parallel across elements of the collection.
# - The  Spark framework allocates data and processing to different nodes, without any intervention from the programmer.
# - RDDs automatically rebuilt on machine failure.
#

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Lifecycle of a Spark Program
# 1. Create some input RDDs from external data or parallelize a collection in your driver program.
# 2. Lazily transform them to define new RDDs using transformations like `filter()` or `map()`
# 3. Ask Spark to cache() any intermediate RDDs that will need to be reused.
# 4. Launch actions such as count() and collect() to kick off a parallel computation, which is then optimized and executed by Spark.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Operations on Distributed Data
# - Two types of operations: **transformations** and **actions**
# - Transformations are *lazy* (not computed immediately) 
# - Transformations are executed when an action is run
#

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # [Transformations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations) (lazy)
# ```spark
# map() flatMap()
# filter() 
# mapPartitions() mapPartitionsWithIndex() 
# sample()
# union() intersection() distinct()
# groupBy() groupByKey()
# reduceBy() reduceByKey()
# sortBy() sortByKey()
# join()
# cogroup()
# cartesian()
# pipe()
# coalesce()
# repartition()
# partitionBy()
# ...
# ```

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # [Actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions)
#
# ```
# reduce()
# collect()
# count()
# first()
# take()
# takeSample()
# saveToCassandra()
# takeOrdered()
# saveAsTextFile()
# saveAsSequenceFile()
# saveAsObjectFile()
# countByKey()
# foreach()
# ```

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # PySpark
#
#
# PySpark uses Py4J that enables Python programs to dynamically access Java objects.
#
# ![PySpark Internals](http://i.imgur.com/YlI8AqEl.png)

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## The `SparkContext` class
#
# - When working with Apache Spark we invoke methods on an object which is an instance of the `pyspark.SparkContext` context.
#
# - Typically, an instance of this object will be created automatically for you and assigned to the variable `sc`.
#
# - The `parallelize` method in `SparkContext` can be used to turn any ordinary Python collection into an RDD;
#     - normally we would create an RDD from a large file or an HBase table. 

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## First example
#
# PySpark isn't on sys.path by default, but that doesn't mean it can't be used as a regular library. You can address this by either symlinking pyspark into your site-packages, or adding pyspark to sys.path at runtime. [findspark](https://github.com/minrk/findspark) does the latter.

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
# We have a spark context sc to use with a tiny local spark cluster with 4 nodes (will work just fine on a multicore machine).
# -

# If you use the workstation in room A111 run the code below before:
#
# ```py
# import findspark
# import os
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64",
# os.environ["SPARK_HOME"] = "/export/spark-2.3.1-bin-hadoop2.7"
#
# findspark.init()
# ```

import sys, findspark
# Set the right value for spark_home
findspark.init(spark_home="/usr/local/opt/apache-spark/libexec")
sys.path

import pyspark

sc = pyspark.SparkContext(master="local[4]", appName="FirstExample")

# + {"slideshow": {"slide_type": "fragment"}}
print(sc) # it is like a Pool Processor executor

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Create your first RDD

# + {"slideshow": {"slide_type": "fragment"}}
rdd = sc.parallelize(list(range(8))) # create collection

# + {"slideshow": {"slide_type": "fragment"}}
rdd
# -

# ### Exercise
#
# Create a file `sample.txt`with lorem package. Read and load it into a RDD with the `textFile` spark function.

# +
import lorem

with open("sample.txt","w") as f:
    f.write(lorem.text())
    
rdd = sc.textFile("sample.txt")
    

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Collect
#
# Action / To Driver: Return all items in the RDD to the driver in a single list
#
# ![](http://i.imgur.com/DUO6ygB.png)
# -

# ### Exercise 
#
# Collect the text you read before from the `sample.txt`file.

rdd.collect()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Map
#
# Transformation / Narrow: Return a new RDD by applying a function to each element of this RDD
#
# ![](http://i.imgur.com/PxNJf0U.png)

# + {"slideshow": {"slide_type": "fragment"}}
rdd = sc.parallelize(list(range(8)))
rdd.map(lambda x: x ** 2).collect() # Square each element
# -

# ### Exercise
#
# Replace the lambda function by a function that contains a pause (sleep(1)) and check if the `map` operation is parallelized.

# +
from time import sleep
def square(x):
    sleep(1)
    return x**2

# %time rdd.map(square).collect()


# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Filter
#
# Transformation / Narrow: Return a new RDD containing only the elements that satisfy a predicate
#
# ![](http://i.imgur.com/GFyji4U.png)

# + {"slideshow": {"slide_type": "fragment"}}
# Select only the even elements
rdd.filter(lambda x: x % 2 == 0).collect()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### FlatMap
#
# Transformation / Narrow: Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results
#
# ![](http://i.imgur.com/TsSUex8.png)

# + {"slideshow": {"slide_type": "fragment"}}
rdd = sc.parallelize([1,2,3])
rdd.flatMap(lambda x: (x, x*100, 42)).collect()
# -

# ### Exercise
#
# Use FlatMap to clean the text from `sample.txt`file. Lower, remove dots and split into words.



# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### GroupBy
#
# Transformation / Wide: Group the data in the original RDD. Create pairs where the key is the output of a user function, and the value is all items for which the function yields this key.
#
# ![](http://i.imgur.com/gdj0Ey8.png)

# + {"slideshow": {"slide_type": "fragment"}}
rdd = sc.parallelize(['John', 'Fred', 'Anna', 'James'])
rdd = rdd.groupBy(lambda w: w[0])
[(k, list(v)) for (k, v) in rdd.collect()]

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### GroupByKey
#
# Transformation / Wide: Group the values for each key in the original RDD. Create a new pair where the original key corresponds to this collected group of values.
#
# ![](http://i.imgur.com/TlWRGr2.png)

# + {"slideshow": {"slide_type": "fragment"}}
rdd = sc.parallelize([('B',5),('B',4),('A',3),('A',2),('A',1)])
rdd = rdd.groupByKey()
[(j[0], list(j[1])) for j in rdd.collect()]

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Join
#
# Transformation / Wide: Return a new RDD containing all pairs of elements having the same key in the original RDDs
#
# ![](http://i.imgur.com/YXL42Nl.png)

# + {"slideshow": {"slide_type": "fragment"}}
x = sc.parallelize([("a", 1), ("b", 2)])
y = sc.parallelize([("a", 3), ("a", 4), ("b", 5)])
x.join(y).collect()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Distinct
#
# Transformation / Wide: Return a new RDD containing distinct items from the original RDD (omitting all duplicates)
#
# ![](http://i.imgur.com/Vqgy2a4.png)

# + {"slideshow": {"slide_type": "fragment"}}
rdd = sc.parallelize([1,2,3,3,4])
rdd.distinct().collect()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### KeyBy
#
# Transformation / Narrow: Create a Pair RDD, forming one pair for each item in the original RDD. The pair’s key is calculated from the value via a user-supplied function.
#
# ![](http://i.imgur.com/nqYhDW5.png)

# + {"slideshow": {"slide_type": "fragment"}}
rdd = sc.parallelize(['John', 'Fred', 'Anna', 'James'])
rdd.keyBy(lambda w: w[0]).collect()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Actions
#
# ### Map-Reduce operation 
#
# Action / To Driver: Aggregate all the elements of the RDD by applying a user function pairwise to elements and partial results, and return a result to the driver
#
# ![](http://i.imgur.com/R72uzwX.png)

# + {"slideshow": {"slide_type": "fragment"}}
from operator import add
rdd = sc.parallelize(list(range(8)))
rdd.map(lambda x: x ** 2).reduce(add) # reduce is an action!
# -

# ### Max, Min, Sum, Mean, Variance, Stdev
#
# Action / To Driver: Compute the respective function (maximum value, minimum value, sum, mean, variance, or standard deviation) from a numeric RDD
#
# ![](http://i.imgur.com/HUCtib1.png)

# ### CountByKey
#
# Action / To Driver: Return a map of keys and counts of their occurrences in the RDD
#
# ![](http://i.imgur.com/jvQTGv6.png)

# +
rdd = sc.parallelize([('J', 'James'), ('F','Fred'), 
                    ('A','Anna'), ('J','John')])

rdd.countByKey()

# + {"slideshow": {"slide_type": "fragment"}}
# Stop the local spark cluster
sc.stop()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Exercise 10.1 Word-count in Apache Spark

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
# - Write the sample text file

# + {"slideshow": {"slide_type": "fragment"}}
from lorem import text
with open('sample.txt','w') as f:
    f.write(text())

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
#
# - Create the rdd with `SparkContext.textFile method`
# - lower, remove dots and split using `rdd.flatMap`
# - use `rdd.map` to create the list of key/value pair (word, 1)
# - `rdd.reduceByKey` to get all occurences
# - `rdd.takeOrdered`to get sorted frequencies of words
#
# All documentation is available [here](https://spark.apache.org/docs/2.1.0/api/python/pyspark.html?highlight=textfile#pyspark.SparkContext) for textFile and [here](https://spark.apache.org/docs/2.1.0/api/python/pyspark.html?highlight=textfile#pyspark.RDD) for RDD. 
#
# For a global overview see the Transformations section of the [programming guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)

# +
import pyspark

sc = pyspark.SparkContext(master="local[4]", appName="wordcount")
# -

rdd = sc.textFile("sample.txt")

(rdd.flatMap(lambda line: line.lower().replace("."," ").split())
   .map(lambda w : (w,1))
   .reduceByKey(lambda w, c: w + c)
   .sortBy( lambda w : -w[1]).collect())

sc.stop()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # SparkSession
#
# Since SPARK 2.0.0,  SparkSession provides a single point 
# of entry to interact with Spark functionality and
# allows programming Spark with DataFrame and Dataset APIs. 
#
# ### $\pi$ computation example
#
# - We can estimate an approximate value for $\pi$ using the following Monte-Carlo method:
#
# 1.    Inscribe a circle in a square
# 2.    Randomly generate points in the square
# 3.    Determine the number of points in the square that are also in the circle
# 4.    Let $r$ be the number of points in the circle divided by the number of points in the square, then $\pi \approx 4 r$.
#     
# - Note that the more points generated, the better the approximation
#
# See [this tutorial](https://computing.llnl.gov/tutorials/parallel_comp/#ExamplesPI).

# + {"slideshow": {"slide_type": "fragment"}}
import sys
from random import random
from operator import add

from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[4]")
         .appName("PythonPi")
         .config("spark.executor.instances", "4")
         .getOrCreate())

partitions = 8
n = 100000 * partitions

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

count = spark.sparkContext.parallelize(range(1, n+1), partitions).map(f).reduce(add)
print("Pi is roughly %f" % (4.0 * count / n))

spark.stop()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Exercise 9.2
#
# Using the same method than the PI computation example, compute the integral
# $$
# I = \int_0^1 \exp(-x^2) dx
# $$
# You can check your result with numpy

# + {"slideshow": {"slide_type": "fragment"}}
# numpy evaluates solution using numeric computation. 
# It uses discrete values of the function
import numpy as np
x = np.linspace(0,1,1000)
np.trapz(np.exp(-x*x),x)

# + {"slideshow": {"slide_type": "fragment"}}
# numpy and scipy evaluates solution using numeric computation. It uses discrete values
# of the function
import numpy as np
from scipy.integrate import quad
quad(lambda x: np.exp(-x*x), 0, 1)
# note: the solution returned is complex 

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Correlation between daily stock
#
# - Data preparation

# +
import os  # library to get directory and file paths
import tarfile # this module makes possible to read and write tar archives

def extract_data(name, where):
    datadir = os.path.join(where,name)
    if not os.path.exists(datadir):
       print("Extracting data...")
       tar_path = os.path.join(where, name+'.tgz')
       with tarfile.open(tar_path, mode='r:gz') as data:
          data.extractall(where)
            
extract_data('daily-stock','../data') # this function call will extract json files

# +
import json
import pandas as pd
import os, glob

here = os.getcwd()
datadir = os.path.join(here,'..','data','daily-stock')
filenames = sorted(glob.glob(os.path.join(datadir, '*.json')))
filenames

# + {"slideshow": {"slide_type": "slide"}}
from glob import glob
import os, json
import pandas as pd

for fn in filenames:
    with open(fn) as f:
        data = [json.loads(line) for line in f]
        
    df = pd.DataFrame(data)
    
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, '/data')
    print("Finished : %s" % out_filename.split(os.path.sep)[-1])

filenames = sorted(glob(os.path.join('..','data', 'daily-stock', '*.h5')))  # ../data/json/*.json

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Sequential code

# + {"slideshow": {"slide_type": "fragment"}}
# %%time

series = []
for fn in filenames:   # Simple map over filenames
    series.append(pd.read_hdf(fn)['close'])

results = []

for a in series:    # Doubly nested loop over the same collection
    for b in series:  
        if not (a == b).all():     # Filter out comparisons of the same series 
            results.append(a.corr(b))  # Apply function

result = max(results)
result

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Exercise 9.3
#
# Parallelize the code above with Apache Spark.
# -

# - Change the filenames because of the Hadoop environment.

# +
import os, glob

here = os.getcwd()
filenames = sorted(glob.glob(os.path.join(here[:-10],'data', 'daily-stock', '*.h5')))
filenames
# -

# If it is not started don't forget the PySpark context

# +
### Parallel code
import pandas as pd

rdd = sc.parallelize(filenames)
series = rdd.map(lambda fn: pd.read_hdf(fn)['close'])

corr = (series.cartesian(series)
              .filter(lambda ab: not (ab[0] == ab[1]).all())
              .map(lambda ab: ab[0].corr(ab[1]))
              .max())

corr

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
# Computation time is slower because there is a lot of setup, workers creation, there is a lot of communications the correlation function is too small

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Exercise 9.4 Fasta file example
#
# Use a RDD to calculate the GC content of fasta file nucleotide-sample.txt:
#
# $$\cfrac{G+C}{A+T+G+C}\times100%$$
#
# Create a rdd from fasta file genome.txt in data directory and count 'G' and 'C' then divide by the total number of bases.
# -

genome = sc.textFile('../data/genome.txt')

lines = genome.flatMap(lambda line: line.split())
g = lines.map(lambda line: line.count("G")).sum()
c = lines.map(lambda line: line.count("C")).sum()
(g + c) / lines.map(lambda line:len(line)).sum()


# ### Another example
#
# Compute the most frequent sequence with 5 bases.

# +
def group_characters(line, n=5):
    result = ''
    i = 0
    for ch in line:
        result = result + ch
        i = i + 1
        if (i % n) == 0:
            yield result
            result = ''

def group_and_split(line):
    return [sequence for sequence in group_characters(line)]

sequences = genome.flatMap(group_and_split)
sequences.take(3)
# -

counts = sequences.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y).sortBy(lambda v:-v[1])
counts.take(10)

sc.stop()
