#!/bin/bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

usage() {
	echo "$0 <from> <to>"
	exit 1
}

[ "$1" == "" ] && usage
[ "$2" == "" ] && usage


inDir="$1"
outDir="$2"

git grep SplitSupport.process | grep "$1" | cut -d ':' -f1 | while read f;do

	echo "processing: $f"
	n="`grep N_SPLITS "$f" | cut -d= -f2 | tr -c -d '0-9'`"
	echo " * nSplits: $n"

	for((i=0;i<n;i++)) {
		oDir="`dirname $f | sed "s|$inDir|$outDir|"`/split$i"
		mkdir -p $oDir
		cat $f | sed -r "s|^(package.*);$|\1.split$i;|g" > $oDir/`basename $f`
	}
done
