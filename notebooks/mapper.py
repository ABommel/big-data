#!/Users/navaro/miniconda3/envs/big-data/bin/python

from __future__ import print_function # for python2 compatibility
import sys, string
translator = str.maketrans('', '', string.punctuation)
# input comes from standard input
for line in sys.stdin:
    line = line.strip().lower() # remove leading and trailing whitespace
    line = line.translate(translator)   # strip punctuation 
    for word in line.split(): # split the line into words
        # write the results to standard output;
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        # tab-delimited; the trivial word count is 1
        print (f'{word}\t 1')
