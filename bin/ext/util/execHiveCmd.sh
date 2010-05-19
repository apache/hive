execHiveCmd () {
  CLASS=$1;
  shift;

  # cli specific code
  if [ ! -f ${HIVE_LIB}/hive-cli-*.jar ]; then
    echo "Missing Hive CLI Jar"
    exit 3;
  fi

  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi

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

  # increase the threashold for large queries
  HADOOP_HEAPSIZE=4096 
  HADOOP_OPTS="$HADOOP_OPTS -XX:-UseGCOverheadLimit"

  if [ $minor_ver -lt 20 ]; then
      exec $HADOOP jar $AUX_JARS_CMD_LINE ${HIVE_LIB}/hive-cli-*.jar $CLASS $HIVE_OPTS "$@"
  else
      # hadoop 20 or newer - skip the aux_jars option. picked up from hiveconf
      exec $HADOOP jar ${HIVE_LIB}/hive-cli-*.jar $CLASS $HIVE_OPTS "$@" 
  fi

}
