THISSERVICE=rcfilecat
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

rcfilecat () {
  CLASS=org.apache.hadoop.hive.cli.RCFileCat
  HIVE_OPTS =''
  execHiveCmd $CLASS "$@"
}

rcfilecat_help () {
  echo "usage ./hive rcfilecat [--start='startoffset'] [--length='len'] "
} 
