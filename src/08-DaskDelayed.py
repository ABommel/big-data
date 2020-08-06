# ---
# jupyter:
#   jupytext:
#     cell_metadata_json: true
#     formats: ipynb,../src//py
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.3.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# <img src="images/dask_logo.jpg">

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# # Dask Features
#
# - process data that doesn't fit into memory by breaking it into blocks and specifying task chains
# - parallelize execution of tasks across cores and even nodes of a cluster
# - move computation to the data rather than the other way around, to minimize communication overheads
#
# http://dask.pydata.org/en/latest/

# + {"slideshow": {"slide_type": "fragment"}}
import dask
import dask.multiprocessing

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# ## Define two slow functions

# + {"slideshow": {"slide_type": "fragment"}}
from time import sleep

def slowinc(x, delay=1):
    sleep(delay)
    return x + 1

def slowadd(x, y, delay=1):
    sleep(delay)
    return x + y


# + {"slideshow": {"slide_type": "fragment"}}
# %%time
x = slowinc(1)
y = slowinc(2)
z = slowadd(x, y)

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# # Parallelize with dask.delayed
#
# - Functions wrapped by `dask.delayed` don't run immediately, but instead put those functions and arguments into a task graph. 
# - The result is computed separately by calling the `.compute()` method.

# + {"slideshow": {"slide_type": "fragment"}}
from dask import delayed

# + {"slideshow": {"slide_type": "fragment"}}
x = delayed(slowinc)(1)
y = delayed(slowinc)(2)
z = delayed(slowadd)(x, y)

# + {"slideshow": {"slide_type": "fragment"}}
# %%time
z.compute()

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# # Dask graph
#
# - Contains description of the calculations necessary to produce the result. 
# - The z object is a lazy Delayed object. This object holds everything we need to compute the final result. We can compute the result with .compute() as above or we can visualize the task graph for this value with .visualize().

# + {"slideshow": {"slide_type": "fragment"}}
z.visualize()

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# # Parallelize a loop
#

# + {"slideshow": {"slide_type": "fragment"}}
# %%time
data = list(range(8))

results = []

for x in data:
    y = slowinc(x)
    results.append(y)

total = sum(results)
total

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# ### Exercise 8.1
#
# - Parallelize this by appending the delayed `slowinc` calls to the list `results`.
# - Display the graph of `total` computation
# - Compute time elapsed for the computation.

# + {"slideshow": {"slide_type": "fragment"}}
from dask import delayed

futures = []

for x in data:
    y = delayed(slowinc)(x)
    futures.append(y)

total = delayed(sum)(futures)
# -
total.visualize()


# %time total.compute()

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# # Decorator
#
# It is also common to see the delayed function used as a decorator. Same example:

# + {"slideshow": {"slide_type": "fragment"}}
# %%time

@dask.delayed
def slowinc(x, delay=1):
    sleep(delay)
    return x + 1

@dask.delayed
def slowadd(x, y, delay=1):
    sleep(delay)
    return x + y

x = slowinc(1)
y = slowinc(2)
z = slowadd(x, y)
z.compute()

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# # Control flow
# -  Delay only some functions, running a few of them immediately. This is helpful when those functions are fast and help us to determine what other slower functions we should call. 
# - In the example below we iterate through a list of inputs. If that input is even then we want to call `half`. If the input is odd then we want to call `odd_process`. This iseven decision to call `half` or `odd_process` has to be made immediately (not lazily) in order for our graph-building Python code to proceed.
#

# + {"slideshow": {"slide_type": "fragment"}}
from random import randint
import dask.delayed

@dask.delayed
def half(x):
    sleep(1)
    return x // 2

@dask.delayed
def odd_process(x):
    sleep(1)
    return 3*x+1

def is_even(x):
    return not x % 2

data = [randint(0,100) for i in range(8)]
data

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# ### Exercise 8.2
# - Parallelize the sequential code above using dask.delayed
# - You will need to delay some functions, but not all
# - Visualize and check the computed result
#

# + {"slideshow": {"slide_type": "slide"}}
results = []
for x in data:
    if is_even(x):
        y = half(x)
    else:
        y = odd_process(x)
    results.append(y)
    
total = delayed(sum)(results)
total.visualize()

# + [markdown] {"slideshow": {"slide_type": "slide"}}
# ### Exercise 8.3
# - Parallelize the hdf5 conversion from json files
# - Create a function `convert_to_hdf`
# - Use dask.compute function on delayed calls of the funtion created list
# - Is it really  faster as expected ?
#
# Hint: Read [Delayed Best Practices](http://dask.pydata.org/en/latest/delayed-best-practices.html)

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

# + {"slideshow": {"slide_type": "fragment"}}
import os, sys
from glob import glob
import pandas as pd
import json

here = os.getcwd() # get the current directory
filenames = sorted(glob(os.path.join(here,'../data', 'daily-stock', '*.json')))

# + {"slideshow": {"slide_type": "slide"}}
from tqdm.notebook import tqdm 

def read( fn ):
    with open(fn) as f:
        return [json.loads(line) for line in f]
    
def convert(data):
    df = pd.DataFrame(data)
    
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, os.path.join(here,'..','data'))
    return

for fn in tqdm(filenames):  
    data = read( fn)
    convert(data)
    
# -

# %ls ../data/daily-stock/*.h5

# +
@dask.delayed
def read( fn ):
    " read json file "
    with open(fn) as f:
        return [json.loads(line) for line in f]
    
@dask.delayed
def convert(data, fn):
    " convert json file to hdf5 file"
    df = pd.DataFrame(data)
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, '/data')
    return fn[:-5]

results = []
for filename in filenames:
    data = read(filename)
    results.append(convert(data, filename))
# -

# %time dask.compute(*results)


