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

# # Hadoop File Formats
#
# [Format Wars: From VHS and Beta to Avro and Parquet](http://www.svds.com/dataformats/)

# ## Feather
#
# For light data, it is recommanded to use [Feather](https://github.com/wesm/feather). It is a fast, interoperable data frame storage that comes with bindings for python and R.
#
# Feather uses also the Apache Arrow columnar memory specification to represent binary data on disk. This makes read and write operations very fast.

import feather
import pandas as pd
import numpy as np
arr = np.random.randn(10000) # 10% nulls
arr[::10] = np.nan
df = pd.DataFrame({'column_{0}'.format(i): arr for i in range(10)})
feather.write_dataframe(df, 'test.feather')

# +
df = pd.read_feather("test.feather")

df.head()
# -

# ## Parquet file format
#
# [Parquet format](https://github.com/apache/parquet-format) is a common binary data store, used particularly in the Hadoop/big-data sphere. It provides several advantages relevant to big-data processing:
#
# - columnar storage, only read the data of interest
# - efficient binary packing
# - choice of compression algorithms and encoding
# - split data into files, allowing for parallel processing
# - range of logical types
# - statistics stored in metadata allow for skipping unneeded chunks
# - data partitioning using the directory structure

# ### Read  `parquet` file with Python
#
# [fastparquet](http://fastparquet.readthedocs.io/en/latest/) provides a performant library to read and write Parquet files from Python, without any need for a Python-Java bridge. This will make the Parquet format an ideal storage mechanism for Python-based big data workflows.
#
# The tabular nature of Parquet is a good fit for the Pandas data-frame objects, and we exclusively deal with data-frame <-> Parquet.
#
# We will use a software layer to access the data. For performance reason the data must be aligned in memory along columns. It is not the default in Python. Apache Arrow will do this for you and improve performance.
#
# Example:
# ```py
# import fastparquet as fp
# pf = fp.ParquetFile('/user/navaro_p/2016-yellow.parquet', open_with=hdfs.open)
# pf
# ```

# ## Apache Arrow
#
# [Arrow](https://arrow.apache.org/docs/python/) is a columnar in-memory analytics layer designed to accelerate big data. It houses a set of canonical in-memory representations of flat and hierarchical data along with multiple language-bindings for structure manipulation.
#
# https://arrow.apache.org/docs/python/parquet.html
#
# The Apache Parquet project provides a standardized open-source columnar storage format for use in data analysis systems. It was created originally for use in Apache Hadoop with systems like Apache Drill, Apache Hive, Apache Impala, and Apache Spark adopting it as a shared standard for high performance data IO.
#
# Apache Arrow is an ideal in-memory transport layer for data that is being read or written with Parquet files. [PyArrow](https://arrow.apache.org/docs/python/) includes Python bindings to read and write Parquet files with pandas.
#
# Example:
# ```py
# import pyarrow as pa
#
# hdfs = pa.hdfs.connect('svmass2.mass.uhb.fr', 54311, 'navaro_p')
# ```

import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa

df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                   'two': ['foo', 'bar', 'baz'],
                   'three': [True, False, True]},
                   index=list('abc'))
table = pa.Table.from_pandas(df)

pq.write_table(table, 'example.parquet')

table2 = pq.read_table('example.parquet')

table2.to_pandas()

pq.read_table('example.parquet', columns=['one', 'three'])

pq.read_pandas('example.parquet', columns=['two']).to_pandas()
