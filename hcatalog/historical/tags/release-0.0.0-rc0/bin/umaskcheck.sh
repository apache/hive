#!/bin/sh

if [ `umask` != '0022' ]
then
  echo "Umask must be set to 0022 to run hcatalog unit tests."
  exit 1
fi
