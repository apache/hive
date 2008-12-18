THISSERVICE=cli
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

cli () {
  CLASS=org.apache.hadoop.hive.cli.CliDriver

  # cli specific code
  if [ ! -f "${HIVE_LIB}/hive_cli.jar" ]; then
    echo "Missing Hive CLI Jar"
    exit 3;
  fi

  exec $HADOOP jar $AUX_JARS_CMD_LINE ${HIVE_LIB}/hive_cli.jar $CLASS $HIVE_OPTS "$@"
}

cli_help () {
  echo "usage ./hive [-e 'hql'| -f file ] "
  echo " -e 'hwl' : execute specified query"
  echo " -f file : exeute query in file"
} 

