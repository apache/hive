#!/bin/bash

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
