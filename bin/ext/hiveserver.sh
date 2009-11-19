THISSERVICE=hiveserver
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

hiveserver() {
  echo "Starting Hive Thrift Server"
  CLASS=org.apache.hadoop.hive.service.HiveServer
  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi
  JAR=${HIVE_LIB}/hive_service.jar

  version=$($HADOOP version | awk '{if (NR == 1) {print $2;}}');

  # Save the regex to a var to workaround quoting incompatabilities
  # between Bash 3.1 and 3.2
  version_re="^([[:digit:]]+)\.([[:digit:]]+)(\.([[:digit:]]+))?.*$"

  if [[ "$version" =~ $version_re ]]; then
      major_ver=${BASH_REMATCH[1]}
      minor_ver=${BASH_REMATCH[2]}
      patch_ver=${BASH_REMATCH[4]}
  else
      echo "Unable to determine Hadoop version information."
      echo "'hadoop version' returned:"
      echo `$HADOOP version`
      exit 6
  fi

  if [ $minor_ver -lt 20 ]; then
    exec $HADOOP jar $AUX_JARS_CMD_LINE $JAR $CLASS $HIVE_PORT "$@"
  else 
    # hadoop 20 or newer - skip the aux_jars option and hiveconf
    exec $HADOOP jar $JAR $CLASS $HIVE_PORT "$@"
  fi
}

hiveserver_help() {
  echo "usage HIVE_PORT=xxxx ./hive --service hiveserver" 
  echo "  HIVE_PORT : Specify the server port"
}

