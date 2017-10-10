#!/bin/bash

THISSERVICE=tokentool
export SERVICE_LIST="${SERVICE_LIST}${THISSERVICE} "

BUFFER_SIZE=20971520 # 20MB.
JAAS_CONF="/home/y/libexec/hcat_server/conf/jaas-hcat-server.conf"
HCAT_SERVER_CONF="/home/y/libexec/hcat_server/conf/hive-site.xml"
CMD_LINE=""

tokentool_help () {
    echo "Usage: hive --tokentool [options]"
    echo "    -bufferSize <int>          : Jute buffer size in bytes (defaults to 20971520, or 20MB)"
    echo "    -jaasConfig <path>         : Path to jaas-config, to connect to the token-store (defaults to /home/y/libexec/hcat_server/conf/jaas-hcat-server.conf)"
    echo "    -confLocation <path>       : Path to the HCat/Hive Server's hive-site.xml (defaults to /home/y/libexec/hcat_server/conf/hive-site.xml)"
    echo "    -delete                    : To delete delegation tokens "
    echo "    -list                      : To list delegation tokens "
    echo "    -expired                   : Select expired tokens for deletion/listing "
    echo "    -olderThan <time-interval> : Select tokens older than the specified time-interval for deletion/listing (e.g. 3d for 3 days, 4h for 4 hours, 5m for 5 minutes, etc.)"
    echo "    -dryRun                    : Don't actually delete, but log which tokens might have been deleted "
    echo "    -batchSize                 : Number of tokens to drop between sleep intervals. "
    echo "    -sleepTime                 : Sleep-time in seconds, between batches of dropped delegation tokens. "
    echo "    -serverMode                : The service from which to read delegation tokens. Should be either of [METASTORE, HIVESERVER2]"
    echo "    -h | --help                : Print this help message, to clarify usage ";
}

tokentool() {
    while [[ $# -gt 0 ]]
    do

    case $1 in
        -bufferSize|--bufferSize)
        BUFFER_SIZE="$2"
        shift # past argument
        ;;
        -jaasConfig|--jaasConfig)
        JAAS_CONF="$2"
        shift # past argument
        ;;
        -confLocation|--confLocation)
        HCAT_SERVER_CONF="$2"
        shift # past argument
        ;;
        -delete|--delete|-list|--list|-dryRun|--dryRun|-expired|--expired)
        CMD_LINE="$CMD_LINE $1"
        ;;
        -olderThan|--olderThan|-batchSize|--batchSize|-sleepTime|--sleepTime|-serverMode|--serverMode)
        CMD_LINE="$CMD_LINE $1 $2"
        shift # past argument
        ;;
        -h|--help)
        tokentool_help
        exit 0;
        ;;
        *)
        echo "Unrecognized option: $1"
        tokentool_help
        exit -1
        ;;
    esac
    shift # past option.
    done # /while

    CMD_LINE="$CMD_LINE --confLocation $HCAT_SERVER_CONF"
    export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Djute.maxbuffer=$BUFFER_SIZE -Djava.security.auth.login.config=$JAAS_CONF"
    execHiveCmd org.apache.hadoop.hive.metastore.security.DelegationTokenTool $CMD_LINE
}
