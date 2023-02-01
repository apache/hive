yarnfile = """
{
  "name": "%(name)s",
  "version": "1.0.0",
  "queue": "%(queue.string)s",
  "configuration": {
    "properties": {
      "yarn.service.rolling-log.include-pattern": ".*\\\\.done",
      "yarn.service.container-health-threshold.percent": "%(health_percent)d",
      "yarn.service.container-health-threshold.window-secs": "%(health_time_window)d",
      "yarn.service.container-health-threshold.init-delay-secs": "%(health_init_delay)d"%(service_appconfig_global_append)s
    }
  },
  "components": [
    {
      "name": "llap",
      "number_of_containers": %(instances)d,
      "launch_command": "$LLAP_DAEMON_BIN_HOME/llapDaemon.sh start &> $LLAP_DAEMON_TMP_DIR/shell.out",
      "artifact": {
        "id": "%(hdfs_package_dir)s/package/LLAP/%(name)s-%(version)s.tar.gz",
        "type": "TARBALL"
      },
      "resource": {
        "cpus": 1,
        "memory": "%(container.mb)d"
      },
      "placement_policy": {
        "constraints": [
          {
            "type": "ANTI_AFFINITY",
            "scope": "node",
            "target_tags": [
              "llap"
            ]
          }
        ]
      },
      "configuration": {
        "env": {
          "JAVA_HOME": "%(java_home)s",
          "LLAP_DAEMON_HOME": "$PWD/lib/",
          "LLAP_DAEMON_TMP_DIR": "$PWD/tmp/",
          "LLAP_DAEMON_BIN_HOME": "$PWD/lib/bin/",
          "LLAP_DAEMON_CONF_DIR": "$PWD/lib/conf/",
          "LLAP_DAEMON_LOG_DIR": "<LOG_DIR>",
          "LLAP_DAEMON_LOGGER": "%(daemon_logger)s",
          "LLAP_DAEMON_LOG_LEVEL": "%(daemon_loglevel)s",
          "LLAP_DAEMON_HEAPSIZE": "%(heap)d",
          "LLAP_DAEMON_PID_DIR": "$PWD/lib/app/run/",
          "LLAP_DAEMON_LD_PATH": "%(hadoop_home)s/lib/native",
          "LLAP_DAEMON_OPTS": "%(daemon_args)s",

          "APP_ROOT": "<WORK_DIR>/app/install/",
          "APP_TMP_DIR": "<WORK_DIR>/tmp/"
        }
      }
    }
  ],
  "kerberos_principal" : {
    "principal_name" : "%(service_principal)s",
    "keytab" : "%(service_keytab_path)s"
  },
  "quicklinks": {
    "LLAP Daemon JMX Endpoint": "http://llap-0.${SERVICE_NAME}.${USER}.${DOMAIN}:15002/jmx"
  }
}
"""

# Placement policy feature like ANTI AFFINITY is not yet merged to trunk in YARN
runner = """
#!/bin/bash -e

BASEDIR=$(dirname $0)
yarn app -stop %(name)s
yarn app -destroy %(name)s
hdfs dfs -mkdir -p %(hdfs_package_dir)s/package/LLAP
hdfs dfs -copyFromLocal -f $BASEDIR/%(name)s-%(version)s.tar.gz %(hdfs_package_dir)s/package/LLAP
yarn app -launch %(name)s $BASEDIR/Yarnfile
"""
