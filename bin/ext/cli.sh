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
  
  for f in ${HADOOP_HOME}/hadoop*core.jar ${HADOOP_HOME}/lib/hadoop*core.jar; do
      if [[ ! -f $f ]]; then
          continue;
      fi
      if [[ $f == *17* ]] || [[ $f == *18* ]] || [[ $f == *19* ]]; then
          exec $HADOOP jar $AUX_JARS_CMD_LINE ${HIVE_LIB}/hive_cli.jar $CLASS $HIVE_OPTS "$@"
      else
          # hadoop 20 or newer - skip the aux_jars option. picked up from hiveconf
          exec $HADOOP jar ${HIVE_LIB}/hive_cli.jar $CLASS $HIVE_OPTS "$@" 
      fi
  done

}

cli_help () {
  echo "usage ./hive [-e 'hql'| -f file ] "
  echo " -e 'hwl' : execute specified query"
  echo " -f file : exeute query in file"
} 

