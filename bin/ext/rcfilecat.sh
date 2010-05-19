THISSERVICE=rcfilecat
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

rcfilecat () {
  CLASS=org.apache.hadoop.hive.cli.RCFileCat
  execHiveCmd $CLASS "$@"
}

rcfilecat_help () {
  echo "usage ./hive rcfilecat [--start='startoffset'] [--length='len'] "
} 
