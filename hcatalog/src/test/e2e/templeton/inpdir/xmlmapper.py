#!/usr/bin/env python
# dftmapper.py
 
import sys
 
list = []
title = "Unknown"
inText = False
for line in sys.stdin:
    line = line.strip()
    if line.find( "<title>" )!= -1:
        title = line[ len( "<title>" ) : -len( "</title>" ) ]
    if line.find( "<text>" ) != -1:
        inText = True
        continue
    if line.find( "</text>" ) != -1:
        inText = False
        continue
    if inText:
        list.append( line )

text = ' '.join( list )
text = text[0:10] + "..." + text[-10:]
print '[[%s]]\t[[%s]]' % (title, text)
