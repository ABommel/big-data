# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python 3.7
#     language: python
#     name: python3
# ---

# + {"slideshow": {"slide_type": "skip"}}
# %matplotlib inline
# %config InlineBackend.figure_format = 'retina'
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

pd.set_option("display.max_rows", 8)
plt.rcParams['figure.figsize'] = (9, 6)

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Create a [DataFrame](https://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe)

# + {"slideshow": {"slide_type": "fragment"}}
dates = pd.date_range('20130101', periods=6)
pd.DataFrame(np.random.randn(6,4), index=dates, columns=list('ABCD'))

# + {"slideshow": {"slide_type": "slide"}}
pd.DataFrame({'A' : 1.,
              'B' : pd.Timestamp('20130102'),
              'C' : pd.Series(1,index=list(range(4)),dtype='float32'),
              'D' : np.arange(4,dtype='int32'),
              'E' : pd.Categorical(["test","train","test","train"]),
              'F' : 'foo' })


# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Load Data from CSV File

# + {"slideshow": {"slide_type": "fragment"}}
url = "https://www.fun-mooc.fr/c4x/agrocampusouest/40001S03/asset/AnaDo_JeuDonnees_TemperatFrance.csv"
french_cities = pd.read_csv(url, delimiter=";", encoding="latin1", index_col=0)
french_cities

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Viewing Data

# + {"slideshow": {"slide_type": "fragment"}}
french_cities.head()

# + {"slideshow": {"slide_type": "slide"}}
french_cities.tail()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Index

# + {"slideshow": {"slide_type": "fragment"}}
french_cities.index

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
# We can rename an index by setting its name.

# + {"slideshow": {"slide_type": "slide"}}
french_cities.index.name = "City"
french_cities.head()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Exercise 
# ## Rename DataFrame Months in English

# + {"slideshow": {"slide_type": "fragment"}}
import locale
import calendar

locale.setlocale(locale.LC_ALL,'en_US')

months = calendar.month_abbr
print(*months)

# + {"slideshow": {"slide_type": "fragment"}}
french_cities.rename(
  columns={ old : new 
           for old, new in zip(french_cities.columns[:12], months[1:])
          if old != new },
  inplace=True)
french_cities.columns
# -

french_cities.rename(columns={'Moye':'Mean'}, inplace=True)

french_cities

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # From a local or remote HTML file
# We can download and extract data about mean sea level stations around the world from the [PSMSL website](http://www.psmsl.org/).

# + {"slideshow": {"slide_type": "fragment"}}
# Needs `lxml`, `beautifulSoup4` and `html5lib` python packages
table_list = pd.read_html("http://www.psmsl.org/data/obtaining/")

# + {"slideshow": {"slide_type": "fragment"}}
# there is 1 table on that page which contains metadata about the stations where 
# sea levels are recorded
local_sea_level_stations = table_list[0]
local_sea_level_stations

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Indexing on DataFrames

# + {"slideshow": {"slide_type": "fragment"}}
french_cities['Lati']  # DF [] accesses columns (Series)

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# `.loc` and `.iloc` allow to access individual values, slices or masked selections:

# + {"slideshow": {"slide_type": "fragment"}}
french_cities.loc['Rennes', "Sep"]

# + {"slideshow": {"slide_type": "fragment"}}
french_cities.loc['Rennes', ["Sep", "Dec"]]

# + {"slideshow": {"slide_type": "fragment"}}
french_cities.loc['Rennes', "Sep":"Dec"]

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Masking

# + {"slideshow": {"slide_type": "fragment"}}
mask = [True, False] * 6 + 5 * [False]
print(french_cities.iloc[:, mask])

# + {"slideshow": {"slide_type": "fragment"}}
print(french_cities.loc["Rennes", mask])

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # New column
#

# + {"slideshow": {"slide_type": "fragment"}}
french_cities["std"] = french_cities.iloc[:,:12].std(axis=1)
french_cities

# + {"slideshow": {"slide_type": "fragment"}}
french_cities = french_cities.drop("std", axis=1) # remove this new column

# + {"slideshow": {"slide_type": "skip"}}
french_cities

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Modifying a dataframe with multiple indexing

# + {"slideshow": {"slide_type": "fragment"}}
# french_cities['Rennes']['Sep'] = 25 # It does not works and breaks the DataFrame
french_cities.loc['Rennes']['Sep'] # = 25 is the right way to do it

# + {"slideshow": {"slide_type": "skip"}}
french_cities

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Transforming datasets

# + {"slideshow": {"slide_type": "fragment"}}
french_cities['Mean'].min(), french_cities['Ampl'].max()

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
# ## Apply
#
# Let's convert the temperature mean from Celsius to Fahrenheit degree.

# + {"slideshow": {"slide_type": "fragment"}}
fahrenheit = lambda T: T*9/5+32
french_cities['Mean'].apply(fahrenheit)

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Sort

# + {"slideshow": {"slide_type": "fragment"}}
french_cities.sort_values(by='Lati')

# + {"slideshow": {"slide_type": "slide"}}
french_cities = french_cities.sort_values(by='Lati',ascending=False)
french_cities

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Stack and unstack
#
# Instead of seeing the months along the axis 1, and the cities along the axis 0, let's try to convert these into an outer and an inner axis along only 1 time dimension.

# + {"slideshow": {"slide_type": "fragment"}}
pd.set_option("display.max_rows", 20)
unstacked = french_cities.iloc[:,:12].unstack()
unstacked

# + {"slideshow": {"slide_type": "subslide"}}
type(unstacked)

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Transpose

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
# The result is grouped in the wrong order since it sorts first the axis that was unstacked. We need to transpose the dataframe.

# + {"slideshow": {"slide_type": "slide"}}
city_temp = french_cities.iloc[:,:12].transpose()
city_temp.plot()

# + {"slideshow": {"slide_type": "slide"}}
city_temp.boxplot(rot=90);

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Describing

# + {"slideshow": {"slide_type": "fragment"}}
french_cities['Région'].describe()

# + {"slideshow": {"slide_type": "fragment"}}
french_cities['Région'].unique()

# + {"slideshow": {"slide_type": "fragment"}}
french_cities['Région'].value_counts()

# + {"slideshow": {"slide_type": "fragment"}}
# To save memory, we can convert it to a categorical column:
french_cities["Région"] = french_cities["Région"].astype("category")

# + {"slideshow": {"slide_type": "skip"}}
french_cities.memory_usage()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Data Aggregation/summarization
#
# ## groupby

# + {"slideshow": {"slide_type": "fragment"}}
fc_grouped_region = french_cities.groupby("Région")
type(fc_grouped_region)

# + {"slideshow": {"slide_type": "fragment"}}
for group_name, subdf in fc_grouped_region:
    print(group_name)
    print(subdf)
    print("")

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # matplotlib
# ```bash
# # %pip install adjusttext
# ```

# + {"slideshow": {"slide_type": "fragment"}}
from adjustText import adjust_text

x, y = french_cities['Lati'],french_cities["Mean"]
labels = french_cities.index
plt.scatter(x, y)
texts = []
for x0, y0, s0 in zip(x,y,labels):
    texts.append(plt.text(x0, y0, s0, size=14))
adjust_text(texts, x, y);
