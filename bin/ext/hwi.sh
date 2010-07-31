THISSERVICE=hwi
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

hwi() {

  if $cygwin; then
    HIVE_LIB=`cygpath -w "$HIVE_LIB"`
  fi

  CLASS=org.apache.hadoop.hive.hwi.HWIServer
  # The ls hack forces the * to be expanded which is required because 
  # System.getenv doesn't do globbing
  export HWI_JAR_FILE=$(ls ${HIVE_LIB}/hive-hwi-*.jar)
  export HWI_WAR_FILE=$(ls ${HIVE_LIB}/hive-hwi-*.war)

  #hwi requires ant jars
  if [ "$ANT_LIB" = "" ] ; then
    ANT_LIB=/opt/ant/lib
  fi
  for f in ${ANT_LIB}/*.jar; do
    if [[ ! -f $f ]]; then
      continue;
    fi
    HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$f
  done

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

  export HADOOP_CLASSPATH
  
  if [ $minor_ver -lt 20 ]; then
    exec $HADOOP jar $AUX_JARS_CMD_LINE ${HWI_JAR_FILE} $CLASS $HIVE_OPTS "$@"
  else
    # hadoop 20 or newer - skip the aux_jars option and hiveconf
    exec $HADOOP jar ${HWI_JAR_FILE} $CLASS $HIVE_OPTS "$@"
  fi
  #nohup $HADOOP jar $AUX_JARS_CMD_LINE ${HWI_JAR_FILE} $CLASS $HIVE_OPTS "$@" >/dev/null 2>/dev/null &

}

hwi_help(){
  echo "Usage ANT_LIB=XXXX hive --service hwi"	
}
