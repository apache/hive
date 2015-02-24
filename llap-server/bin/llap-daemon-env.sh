# Path to the directory which containts hive-exec and hive-llap-server
#export LLAP_DAEMON_HOME=

#Path to the directory containing llap configs. Eventually, merge with hive-site.
#export LLAP_DAEMON_CONF_DIR=

# Heap size in MB for the llap-daemon. Determined by #executors, #memory-per-executor setup in llap-daemon-configuration.
#export LLAP_DAEMON_HEAPSIZE=

# Path to the BIN scripts. Ideally this should be the same as the hive bin directories.
#export LLAP_DAEMON_BIN_HOME=

# Set this to a path containing tez jars
#export LLAP_DAEMON_USER_CLASSPATH=

# Logger setup for LLAP daemon
#export LLAP_DAEMON_LOGGER=INFO,RFA

# Directory to which logs will be generated
#export LLAP_DAEMON_LOG_DIR=

# Directory in which the pid file will be generated
#export LLAP_DAEMON_PID_DIR=

# Additional JAVA_OPTS for the daemon process
#export LLAP_DAEMON_OPTS=
