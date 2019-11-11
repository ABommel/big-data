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
# <img src="images/dask_logo.jpg">

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
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

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
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

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
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

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Dask graph
#
# - Contains description of the calculations necessary to produce the result. 
# - The z object is a lazy Delayed object. This object holds everything we need to compute the final result. We can compute the result with .compute() as above or we can visualize the task graph for this value with .visualize().

# + {"slideshow": {"slide_type": "fragment"}}
z.visualize()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
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

# + {"slideshow": {"slide_type": "fragment"}}
from dask import delayed

futures = []

for x in data:
    y = delayed(slowinc)(x)
    futures.append(y)

total = delayed(sum)(futures)
# -



# %time total.compute()



total.visualize()


# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Exercise 8.1
#
# - Parallelize this by appending the delayed `slowinc` calls to the list `results`.
# - Display the graph of `total` computation
# - Compute time elapsed for the computation.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
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

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
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

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
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

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Exercise 8.3
# - Parallelize the hdf5 conversion from json files
# - Create a function `convert_to_hdf`
# - Use dask.compute function on delayed calls of the funtion created list
# - Is it really  faster as expected ?
#
# Hint: Read [Delayed Best Practices](http://dask.pydata.org/en/latest/delayed-best-practices.html)

# + {"slideshow": {"slide_type": "fragment"}}
import os, sys
from glob import glob
import pandas as pd
import json

here = os.getcwd() # get the current directory
filenames = sorted(glob(os.path.join(here,'..','data', 'daily-stock', '*.json')))

# + {"slideshow": {"slide_type": "slide"}}
from tqdm import tqdm_notebook as tqdm

def read( fn ):
    
    with open(fn) as f:
        return [json.loads(line) for line in f]
    
def convert(data):
    df = pd.DataFrame(data)
    
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, '/data')
    return

for fn in tqdm(filenames):
    
    data = read( fn)
    convert(data)
    
# -

# %ls ../data/daily-stock/*.h5

# %rm ../data/daily-stock/*.h5

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

# %ls ../data/daily-stock/*.h5
