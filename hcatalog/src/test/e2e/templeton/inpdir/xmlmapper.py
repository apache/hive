#!/usr/bin/env python
# dftmapper.py

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
 
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
