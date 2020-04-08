#!/bin/bash
#Developer needs to make sure HMS API field in hive_metastore.thrift file
#is updated whenever there is a change in the thrift file, e.g. const string HMS_API = "1.0.1"
#then paste in the md5 checksum for it. md5 hive_metastore.thrift >> versionmap.txt
#optionally, but preferrably attach the version.
pushd $(dirname $0)
echo ${PWD}
echo "Checking HMS API version"
vfile=./versionmap.txt
checksum=$(python3  ./checkmd5.py ./src/main/thrift/hive_metastore.thrift)
if [ -e $vfile ]; then 
   cat $vfile | grep $checksum
   result=$?
   if [ $result -eq 0 ]; then
      popd
      exit 0
   else
      popd
      exit 1 
   fi
fi
popd