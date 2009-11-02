THISSERVICE=cli
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

cli () {
  CLASS=org.apache.hadoop.hive.cli.CliDriver

  # cli specific code
  if [ ! -f "${HIVE_LIB}/hive_cli.jar" ]; then
    echo "Missing Hive CLI Jar"
    exit 3;
  fi

  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi

  version=$($HADOOP version | awk '{if (NR == 1) {print $2;}}');

  if [[ $version =~ ^([[:digit:]]+)\.([[:digit:]]+)\.([[:digit:]]+).*$ ]]; then
      major_ver=${BASH_REMATCH[1]}
      minor_ver=${BASH_REMATCH[2]}
      patch_ver=${BASH_REMATCH[3]}
  else
      echo "Unable to determine Hadoop version information."
      echo "'hadoop version' returned:"
      echo `$HADOOP version`
      exit 6
  fi

  if [ $minor_ver -le 20 ]; then
      exec $HADOOP jar $AUX_JARS_CMD_LINE ${HIVE_LIB}/hive_cli.jar $CLASS $HIVE_OPTS "$@"
  else
      # hadoop 20 or newer - skip the aux_jars option. picked up from hiveconf
      exec $HADOOP jar ${HIVE_LIB}/hive_cli.jar $CLASS $HIVE_OPTS "$@" 
  fi

}

cli_help () {
  echo "usage ./hive [-e 'hql'| -f file ] "
  echo " -e 'hwl' : execute specified query"
  echo " -f file : exeute query in file"
} 

