THISSERVICE=hiveserver
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

hiveserver() {
  echo "Starting Hive Thrift Server"
  CLASS=org.apache.hadoop.hive.service.HiveServer
  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi
  JAR=${HIVE_LIB}/hive_service.jar
  if [ "$HIVE_PORT" != "" ]; then
    HIVE_OPTS=$HIVE_PORT
  fi
  exec $HADOOP jar $AUX_JARS_CMD_LINE $JAR $CLASS $HIVE_OPTS "$@"
}

hiveserver_help() {
  echo "usage HIVE_PORT=xxxx ./hive --service hiveserver" 
  echo "  HIVE_PORT : Specify the server port"
}

