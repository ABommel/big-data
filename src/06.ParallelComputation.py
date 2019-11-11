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
# # Parallel Computation
#
# ## Parallel computers
# - Multiprocessor/multicore: several processors work on data stored in shared memory
# - Cluster: several processor/memory units work together by exchanging data over a network
# - Co-processor: a general-purpose processor delegates specific tasks to a special-purpose processor (GPU)
#

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Parallel Programming
# - Decomposition of the complete task into independent subtasks and the data flow between them.
# - Distribution of the subtasks over the processors minimizing the total execution time.
# - For clusters: distribution of the data over the nodes minimizing the communication time.
# - For multiprocessors: optimization of the memory access patterns minimizing waiting times.
# - Synchronization of the individual processes.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## MapReduce

# + {"slideshow": {"slide_type": "fragment"}}
from time import sleep
def f(x):
    sleep(1)
    return x*x
L = list(range(8))
L

# + {"slideshow": {"slide_type": "fragment"}}
# %time sum([f(x) for x in L])

# + {"slideshow": {"slide_type": "fragment"}}
# %time sum(map(f,L))

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Multiprocessing 
#
# `multiprocessing` is a package that supports spawning processes.
#
# We can use it to display how many concurrent processes you can launch on your computer.

# + {"slideshow": {"slide_type": "fragment"}}
from multiprocessing import cpu_count

cpu_count()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Futures
#
# The `concurrent.futures` module provides a high-level interface for asynchronously executing callables.
#
# The asynchronous execution can be performed with:
# - **threads**, using ThreadPoolExecutor, 
# - separate **processes**, using ProcessPoolExecutor. 
# Both implement the same interface, which is defined by the abstract Executor class.
#
# `concurrent.futures` can't launch **processes** on windows. Windows users must install 
# [loky](https://github.com/tomMoral/loky).

# + {"slideshow": {"slide_type": "slide"}}
# %%time
from concurrent.futures import ProcessPoolExecutor
# from loky import ProcessPoolExecutor  # for Windows users

with ProcessPoolExecutor() as epool: # Create several Python processes (cpu_count)

    results = sum(epool.map(f, L))
    
print(results)

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# - `ProcessPoolExecutor` launches one slave process per physical core on the computer. 
# - `epool.map` divides the input list into chunks and puts the tasks (function + chunk) on a queue.
# - Each slave process takes a task (function + a chunk of data), runs map(function, chunk), and puts the result on a result list.
# - `epool.map` on the master process waits until all tasks are handled and returns the concatenation of the result lists.

# + {"slideshow": {"slide_type": "slide"}}
# %%time
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor() as epool:

    results = sum(epool.map(f, L))
    
print(results)

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Thread and Process: Differences
#
# - A **process** is an instance of a running program. 
# - **Process** may contain one or more **threads**, but a **thread** cannot contain a **process**.
# - **Process** has a self-contained execution environment. It has its own memory space. 
# - Application running on your computer may be a set of cooperating **processes**.
# - **Process** don't share its memory, communication between **processes** implies data serialization.
#
# - A **thread** is made of and exist within a **process**; every **process** has at least one **thread**. 
# - Multiple **threads** in a **process** share resources, which helps in efficient communication between **threads**.
# - **Threads** can be concurrent on a multi-core system, with every core executing the separate **threads** simultaneously.
#
#
#

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## The Global Interpreter Lock (GIL)
#
# - The Python interpreter is not thread safe.
# - A few critical internal data structures may only be accessed by one thread at a time. Access to them is protected by the GIL.
# - Attempts at removing the GIL from Python have failed until now. The main difficulty is maintaining the C API for extension modules.
# - Multiprocessing avoids the GIL by having separate processes which each have an independent copy of the interpreter data structures.
# - The price to pay: serialization of tasks, arguments, and results.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Weighted mean and Variance
#
# ### Exercise 6.1
#
# Use `ThreadPoolExecutor` to parallelized functions written in notebook 05

# +
from glob import glob
from collections import defaultdict
from operator import itemgetter
from itertools import chain
from concurrent.futures import ThreadPoolExecutor


def mapper(filename):
    " split text to list of key/value pairs (word,1)"
    with open(filename) as f:
        data = f.read()
        
    data = data.strip().replace(".","").lower().split()
        
    return sorted([(w,1) for w in data])


def partitioner(mapped_values):
    """ get lists from mapper and create a dict with
    (word,[1,1,1])"""
    
    res = defaultdict(list)
    for w, c in mapped_values:
        res[w].append(c)
        
    return res.items()


def reducer( item ):
    """ Compute words occurences from dict computed
    by partioner
    """
    w, v = item
    return (w,len(v))

files = glob("sample*.txt")


def wordcount(files):
    
    with ThreadPoolExecutor() as e:
    
        mapped_values = e.map(mapper, files)
        partioned_values = partitioner(chain(*mapped_values))
        occurences = e.map(reducer, partioned_values)
    
    return sorted(occurences,
                 key=itemgetter(1),
                 reverse=True)
    
    
wordcount(files)    

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Wordcount

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
#
# ## Parallel map
#
#
# - Let's improve the `mapper` function by print out inside the function the current process name. 
#
# *Example*

# + {"slideshow": {"slide_type": "fragment"}}
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
#from loky import ProcessPoolExecutor 
def process_name(n):
    " prints out the current process name "
    print(mp.current_process().name)

with ProcessPoolExecutor() as e:
    _ = e.map(process_name, range(mp.cpu_count()))


# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Exercise 6.2
#
# - Modify the mapper function by adding this print.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Parallel reduce
#
# - For parallel reduce operation, data must be aligned in a container. We already created a `partitioner` function that returns this container.
#
# ### Exercise 6.3
#
# Write a parallel program that uses the three functions above using `ProcessPoolExecutor`. It reads all the "sample\*.txt" files. Map and reduce steps are parallel.
#

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Increase volume of data
#
# *Due to the proxy, code above is not runnable on workstations*
#
# ### Getting the data
#
# - [The Latin Library](http://www.thelatinlibrary.com/) contains a huge collection of freely accessible Latin texts. We get links on the Latin Library's homepage ignoring some links that are not associated with a particular author.
#
# ```py
# from bs4 import BeautifulSoup  # web scraping library
# from urllib.request import *
#
# base_url = "http://www.thelatinlibrary.com/"
# home_content = urlopen(base_url)
#
# soup = BeautifulSoup(home_content, "lxml")
# author_page_links = soup.find_all("a")
# author_pages = [ap["href"] for i, ap in enumerate(author_page_links) if i < 49]
# ```

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Generate html links
#
# - Create a list of all links pointing to Latin texts. The Latin Library uses a special format which makes it easy to find the corresponding links: All of these links contain the name of the text author.
#
# ```py 
# ap_content = list()
# for ap in author_pages:
#     ap_content.append(urlopen(base_url + ap))
#
# book_links = list()
# for path, content in zip(author_pages, ap_content):
#     author_name = path.split(".")[0]
#     ap_soup = BeautifulSoup(content, "lxml")
#     book_links += ([link for link in ap_soup.find_all("a", {"href": True}) if author_name in link["href"]])
#
# ```

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Download webpages content
# ```py
# from urllib.error import HTmTPError
#
# num_pages = 100
#
# for i, bl in enumerate(book_links[:num_pages]):
#     print("Getting content " + str(i + 1) + " of " + str(num_pages), end="\r", flush=True)
#     try:
#         content = urlopen(base_url + bl["href"]).read()
#         with open(f"book-{i:03d}.dat","wb") as f:
#             f.write(content)
#     except HTTPError as err:
#         print("Unable to retrieve " + bl["href"] + ".")
#         continue
# ```

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Extract data files
#
# - I already put the content of pages in files named book-*.txt
# - You can extract data from the archive by running the cell below
#

# + {"slideshow": {"slide_type": "fragment"}}
import os  # library to get directory and file paths
import tarfile # this module makes possible to read and write tar archives

def extract_data():
    datadir = os.path.join('..','data','latinbooks')
    if not os.path.exists(datadir):
       print("Extracting data...")
       tar_path = os.path.join('..','data', 'latinbooks.tgz')
       with tarfile.open(tar_path, mode='r:gz') as books:
          books.extractall('../data')
            
extract_data() # this function call will extract text files in ../data/latinbooks

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Read data files

# + {"slideshow": {"slide_type": "fragment"}}
from glob import glob
files = glob('../data/latinbooks/*')
texts = list()
for file in files:
    with open(file,'rb') as f:
        text = f.read()
    texts.append(text)

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Extract the text from html and split the text at periods to convert it into sentences.

# + {"slideshow": {"slide_type": "fragment"}}
# %%time
from bs4 import BeautifulSoup

sentences = list()

for i, text in enumerate(texts):
    print("Document " + str(i + 1) + " of " + str(len(texts)), end="\r", flush=True)
    textSoup = BeautifulSoup(text, "lxml")
    paragraphs = textSoup.find_all("p", attrs={"class":None})
    prepared = ("".join([p.text.strip().lower() for p in paragraphs[1:-1]]))
    for t in prepared.split("."):
        part = "".join([c for c in t if c.isalpha() or c.isspace()])
        sentences.append(part.strip())

print(sentences[0])

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ### Exercise 6.4
#
# Parallelize this last process using `concurrent.futures`.

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## References
#
# - [Using Conditional Random Fields and Python for Latin word segmentation](https://medium.com/@felixmohr/using-python-and-conditional-random-fields-for-latin-word-segmentation-416ca7a9e513)
