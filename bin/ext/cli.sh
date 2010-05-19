THISSERVICE=cli
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

cli () {
  CLASS=org.apache.hadoop.hive.cli.CliDriver
  execHiveCmd $CLASS "$@"
}

cli_help () {
  echo "usage ./hive [-e 'hql'| -f file ] "
  echo " -e 'hwl' : execute specified query"
  echo " -f file : exeute query in file"
} 

