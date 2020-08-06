#!/usr/bin env python
import sys

# input comes from standard input
k = 0
for file in sys.stdin:
    k +=1
    print('file {} : {}'.format(k,file))
