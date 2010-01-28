THISSERVICE=help
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

help() {
  echo "Usage ./hive <parameters> --service serviceName <service parameters>"
  echo "Service List: $SERVICE_LIST"
  echo "Parameters parsed:"
  echo "  --auxpath : Auxillary jars "
  echo "  --config : Hive configuration directory"
  echo "  --service : Starts specific service/component. cli is default"
  echo "Parameters used:"
  echo "  HADOOP_HOME : Hadoop install directory"
  echo "  HIVE_OPT : Hive options"
  echo "For help on a particular service:"
  echo "  ./hive --service serviceName --help"
}

help_help(){
  help
}

