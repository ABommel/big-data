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
#     display_name: big-data
#     language: python
#     name: big-data
# ---

# ![jupyter](images/jupyter-logo.png)

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Launch Jupyter server
#
# ```bash
# jupyter notebook
# ```
#
#
# - Go to notebooks folder
# - Open the file 03.JupyterQuickStart.ipynb

# + {"slideshow": {"slide_type": "fragment"}, "cell_type": "markdown"}
# # Make a Copy
#
# Before modifying the notebook, make a copy of it. Go to to `File` menu
# on the top left of the notebook and click on `Make a Copy...`

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# # Jupyter Notebook
#
# Jupyter notebook, formerly known as the IPython notebook, is a flexible tool that helps you create readable analyses, as you can keep code, images, comments, formulae and plots together.
#
# Jupyter is quite extensible, supports many programming languages and is easily hosted on your computer or on almost any server — you only need to have ssh or http access. Best of all, it's completely free.
#
# The name Jupyter is an indirect acronyum of the three core languages it was designed for: **JU**lia, **PYT**hon, and **R**

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Keyboard Shortcuts
#
# - To access keyboard shortcuts, use the command palette: `Cmd + Shift + P`
#
# - `Esc` will take you into command mode where you can navigate around your notebook with arrow keys.
# - While in command mode:
#    - A to insert a new cell above the current cell, B to insert a new cell below.
#    - M to change the current cell to Markdown, Y to change it back to code
#    - D + D (press the key twice) to delete the current cell
#
#

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Easy links to documentation
#
# - Shift + Tab will also show you the Docstring

# + {"slideshow": {"slide_type": "fragment"}}
dict

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Magic commands
#
#

# + {"slideshow": {"slide_type": "fragment"}}
# %lsmagic

# + {"slideshow": {"slide_type": "slide"}}
# %ls

# + {"slideshow": {"slide_type": "slide"}}
# %%file sample.txt

write the cell content to the file sample.txt.
The file is created when you run this cell.

# + {"slideshow": {"slide_type": "fragment"}}
# %cat sample.txt

# + {"slideshow": {"slide_type": "slide"}}
# %%file fibonacci.py

f1, f2 = 1, 1
for n in range(10):
    print(f1, end=',')
    f1, f2 = f2, f1+f2

# + {"slideshow": {"slide_type": "fragment"}}
# %run fibonacci.py

# + {"slideshow": {"slide_type": "slide"}}
# # %load fibonacci.py

f1, f2 = 1, 1
for n in range(10):
    print(f1, end=',')
    f1, f2 = f2, f1+f2

# + {"slideshow": {"slide_type": "fragment"}}
# %%time
f1, f2 = 1, 1
for n in range(10):
    print(f1, end=',')
    f1, f2 = f2, f1+f2
print()

# + {"slideshow": {"slide_type": "slide"}, "cell_type": "markdown"}
# ## Installing Python Packages from a Jupyter Notebook
#
# ### Install a conda package in the current Jupyter kernel
#
# Example with package `numpy` from *conda-forge*

# + {"slideshow": {"slide_type": "slide"}}
# %conda install -c conda-forge lorem
