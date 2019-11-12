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
#     display_name: big-data
#     language: python
#     name: big-data
# ---

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Dask bag
#
# Dask proposes "big data" collections with a small set of high-level primitives like `map`, `filter`, `groupby`, and `join`.  With these common patterns we can often handle computations that are more complex than map, but are still structured.
#
# - Dask-bag excels in processing data that can be represented as a sequence of arbitrary inputs ("messy" data)
# - When you encounter a set of data with a format that does not enforce strict structure and datatypes.
#
# **Related Documentation**
#
# *  [Bag Documenation](http://dask.pydata.org/en/latest/bag.html)
# *  [Bag API](http://dask.pydata.org/en/latest/bag-api.html)

# + {"slideshow": {"slide_type": "slide"}}
data = list(range(1,9))
data

# + {"slideshow": {"slide_type": "fragment"}}
import dask.bag as db

b = db.from_sequence(data)

# + {"slideshow": {"slide_type": "slide"}}
b.compute()  # Gather results back to local process
# -

b.map(lambda x : x//2).compute() # compute length of each element and collect results

# +
from time import sleep

def slow_half( x):
    sleep(1)
    return x // 2

res = b.map(slow_half)
res
# -

# %%time
res.compute()

res.visualize()

b.topk

b.product(b).compute() # Cartesian product of each pair 
# of elements in two sequences (or the same sequence in this case)

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# Chain operations to construct more complex computations

# + {"slideshow": {"slide_type": "fragment"}}
(b.filter(lambda x: x % 2 > 0)
  .product(b)
  .filter( lambda v : v[0] % v[1] == 0 and v[0] != v[1])
  .compute())
# -



# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Daily stock example
#
# Let's use the bag interface to read the json files containing time series.
#
# Each line is a JSON encoded dictionary with the following keys
# - timestamp: Day.
# - close: Stock value at the end of the day.
# - high: Highest value.
# - low: Lowest value.
# - open: Opening price.

# +
# preparing data
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
# -

# %ls ../data/daily-stock/*.json

# + {"slideshow": {"slide_type": "fragment"}}
import dask.bag as db
import json
stocks = db.read_text('../data/daily-stock/*.json')

# + {"slideshow": {"slide_type": "fragment"}}
stocks.npartitions

# + {"slideshow": {"slide_type": "slide"}}
stocks.visualize()

# + {"slideshow": {"slide_type": "slide"}}
import json
js = stocks.map(json.loads)


# + {"slideshow": {"slide_type": "slide"}}
import os, sys
from glob import glob
import pandas as pd
import json

here = os.getcwd() # get the current directory
filenames = sorted(glob(os.path.join(here,'..','data', 'daily-stock', '*.json')))
filenames

# + {"slideshow": {"slide_type": "slide"}}
from tqdm import tqdm_notebook as tqdm
for fn in tqdm(filenames):
    with open(fn) as f:
        data = [json.loads(line) for line in f]
        
    df = pd.DataFrame(data)
    
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, '/data')

# + {"slideshow": {"slide_type": "slide"}}
filenames = sorted(glob(os.path.join(here,'..','data', 'daily-stock', '*.h5')))
filenames

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Serial version

# + {"slideshow": {"slide_type": "fragment"}}
# %%time
series = {}
for fn in filenames:   # Simple map over filenames
    series[fn] = pd.read_hdf(fn)['close']

results = {}

for a in filenames:    # Doubly nested loop over the same collection
    for b in filenames:  
        if a != b:     # Filter out bad elements
            results[a, b] = series[a].corr(series[b])  # Apply function

((a, b), corr) = max(results.items(), key=lambda kv: kv[1])  # Reduction
# -

a, b, corr

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Dask.bag methods
#
# We can construct most of the above computation with the following dask.bag methods:
#
# *  `collection.map(function)`: apply function to each element in collection
# *  `collection.product(collection)`: Create new collection with every pair of inputs
# *  `collection.filter(predicate)`: Keep only elements of colleciton that match the predicate function
# *  `collection.max()`: Compute maximum element
#

# + {"slideshow": {"slide_type": "slide"}}
# %%time

import dask.bag as db

b = db.from_sequence(filenames)
series = b.map(lambda fn: pd.read_hdf(fn)['close'])

corr = (series.product(series)
              .filter(lambda ab: not (ab[0] == ab[1]).all())
              .map(lambda ab: ab[0].corr(ab[1])).max())

# + {"slideshow": {"slide_type": "slide"}}
# %%time

result = corr.compute()

# + {"slideshow": {"slide_type": "fragment"}}
result

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Wordcount with Dask bag

# +
import lorem

lorem.text()

# +
import lorem

for i in range(20):
    with open(f"sample{i:02d}.txt","w") as f:
        f.write(lorem.text())
# -

# %ls *.txt

import glob
glob.glob('sample*.txt')

str.



# + {"slideshow": {"slide_type": "fragment"}}
import dask.bag as db
import glob
b = db.read_text(glob.glob('sample*.txt'))

wordcount = (b.str.replace(".","")  # remove dots
             .str.lower()           # lower text
             .str.strip()           # remove \n and trailing spaces
             .str.split()           # split into words
             .flatten()             # chain all words lists
             .frequencies()         # compute occurences
             .topk(10, lambda x: x[1])) # sort and return top 10 words


wordcount.compute() # Run all tasks and return result

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Genome example
# We will use a Dask bag to calculate the frequencies of sequences of five bases, and then sort the sequences into descending order ranked by their frequency.
#
# - First we will define some functions to split the bases into sequences of a certain size

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
# ### Exercise 9.1
#
# - Implement a function `group_characters(line, n=5)` to group `n` characters together and return a iterator. `line` is a text line in genome.txt file.
#
# ```py
# >>> line = "abcdefghijklmno"
# >>> for seq in group_character(line, 5):
#         print(seq)
#         
# "abcde"
# "efghi"
# "klmno"
# ```
#
#     
# - Implement `group_and_split(line)`
# ```py
# >>> group_and_split('abcdefghijklmno')
# ['abcde', 'fghij', 'klmno']
# ```
#
# - Use the dask bag to compute  the frequencies of sequences of five bases.
# -

from string import ascii_lowercase as alphabet
alphabet


# +
def reverse(text):
    k = len(text)
    while k > 0:
        k = k-1
        yield text[k]
    
reverse_alphabet = reverse(alphabet)
print(*reverse_alphabet)


# -

class Reverse:
    
    def __init__(self, data):
        self.data = data
        self.index = len(data)
        
    def __iter__(self):
        return self

    def __next__(self):
        self.index = self.index-1
        if self.index < 0:
            raise StopIteration
        else:
            return self.data[self.index]


# +
class Fibonacci:
    
    def __init__(self, n):
        self.n = n
        self.f0 = 0
        self.f1 = 1
        
    def __iter__(self):
        return self
    
    def __next__(self):
        self.n = self.n - 1
        if self.n < 0:
            raise StopIteration
        else:
            self.f0, self.f1 = self.f1, self.f0 + self.f1
        return self.f1
    
print(*Fibonacci(7))
# -

for c in Reverse(alphabet):
    print(c, end="")

# +
    
for c in reverse(alphabet):
    print(c, end="")
# -



def group_character( line, n=5):
    bases = ''
    for i, b in enumerate(line):
        bases += b
        if (i+1) % n == 0:
            yield bases
            bases = ''


line = "abcdefghijklmno"
for seq in group_character(line, 5):
    print(seq)


def group_and_split( line, n):
    return [seq for seq in group_character(line,n)]

# %ls ../data



# +
import os
from glob import glob

data_path = os.path.join("..","data")
with open(os.path.join(data_path,"genome.txt")) as g:
    data = g.read()
    for i in range(8):
        file = os.path.join(data_path,f"genome{i:02d}.txt")
        with open(file,"w") as f:
            f.write(data)

glob("../data/genome0*.txt")

# +
from operator import itemgetter

import dask.bag as db

b = db.read_text("../data/genome0*.txt")

result = (b.filter(lambda line: not line.startswith(">"))
  .map(lambda line: line.strip())
  .map(lambda line : group_and_split(line,5))
  .flatten()
  .frequencies()
  .topk(10,lambda v : v[1]))
# -

result.visualize()

result.compute()



# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Exercise 9.2
#
# The [FASTA](http://www.cbs.dtu.dk/services/NetGene2/fasta.php) file format is used to write several genome sequences.
#
# - Create a function that can read a [FASTA file](../data/nucleotide-sample.txt) and compute the frequencies for n = 5 of a given sequence.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Exercise 9.3
#
# Write a program that uses the function implemented above to read several FASTA files stored in a Dask bag.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Some remarks about bag
#
# *  Higher level dask collections include functions for common patterns
# *  Move data to collection, construct lazy computation, trigger at the end
# *  Use Dask.bag (`product + map`) to handle nested for loop
#
# Bags have the following known limitations
#
# 1.  Bag operations tend to be slower than array/dataframe computations in the
#     same way that Python tends to be slower than NumPy/Pandas
# 2.  ``Bag.groupby`` is slow.  You should try to use ``Bag.foldby`` if possible.
#     
# 3. Check the [API](http://dask.pydata.org/en/latest/bag-api.html) 
#
# 4. `dask.dataframe` can be faster than `dask.bag`.  But sometimes it is easier to load and clean messy data with a bag. We will see later how to transform a bag into a `dask.dataframe` with the [to_dataframe](http://dask.pydata.org/en/latest/bag-api.html#dask.bag.Bag.to_dataframe) method.
