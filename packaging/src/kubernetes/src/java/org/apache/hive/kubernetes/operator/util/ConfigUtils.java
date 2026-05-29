/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.kubernetes.operator.util;

import java.util.Map;

public final class ConfigUtils {

  private ConfigUtils() {
  }

  public static final String METASTORE_THRIFT_PORT_KEY = "metastore.thrift.port";
  public static final String METASTORE_THRIFT_PORT_HIVE_KEY = "hive.metastore.port";
  public static final int METASTORE_THRIFT_PORT_DEFAULT = 9083;

  public static final String METASTORE_WAREHOUSE_KEY = "metastore.warehouse.dir";

  public static final String METASTORE_CONNECTION_URL_KEY = "javax.jdo.option.ConnectionURL";

  public static final String METASTORE_CONNECTION_DRIVER_KEY = "javax.jdo.option.ConnectionDriverName";

  public static final String METASTORE_CONNECTION_USER_KEY = "javax.jdo.option.ConnectionUserName";

  public static final String METASTORE_URIS_KEY = "hive.metastore.uris";

  public static final String HIVE_METASTORE_WAREHOUSE_KEY = "hive.metastore.warehouse.dir";

  public static final String HIVE_SERVER2_ENABLE_DOAS_KEY = "hive.server2.enable.doAs";

  public static final String HIVE_TEZ_EXEC_INPLACE_PROGRESS_KEY = "hive.tez.exec.inplace.progress";

  public static final String HIVE_TEZ_EXEC_SUMMARY_KEY = "hive.tez.exec.print.summary";

  public static final String HIVE_JAR_DIRECTORY_KEY = "hive.jar.directory";

  public static final String HIVE_USER_INSTALL_DIR_KEY = "hive.user.install.directory";

  public static final String HIVE_LOCAL_SCRATCH_DIR_KEY = "hive.exec.local.scratchdir";

  public static final String HIVE_SERVER2_TEZ_USE_EXTERNAL_SESSIONS_KEY = "hive.server2.tez.use.external.sessions";

  public static final String HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE_KEY =
      "hive.server2.tez.external.sessions.namespace";

  public static final String HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_REGISTRY_CLASS_KEY =
      "hive.server2.tez.external.sessions.registry.class";

  public static final String HIVE_ZOOKEEPER_QUORUM_KEY = "hive.zookeeper.quorum";

  public static final String HIVE_EXECUTION_MODE_KEY = "hive.execution.mode";

  public static final String HIVE_LLAP_EXECUTION_MODE_KEY = "hive.llap.execution.mode";

  public static final String HIVE_LLAP_DAEMON_SERVICE_HOSTS_KEY = "hive.llap.daemon.service.hosts";

  public static final String HIVE_LLAP_DAEMON_MEMORY_MB_KEY = "hive.llap.daemon.memory.per.instance.mb";

  public static final String HIVE_LLAP_DAEMON_NUM_EXECUTORS_KEY = "hive.llap.daemon.num.executors";

  public static final String METASTORE_SERVER_TRANSPORT_MODE_KEY = "metastore.server.thrift.transport.mode";
  public static final String METASTORE_SERVER_TRANSPORT_MODE_DEFAULT = "http";

  public static final String METASTORE_SERVER_HTTP_PATH_KEY = "metastore.server.thrift.http.path";
  public static final String METASTORE_SERVER_HTTP_PATH_DEFAULT = "metastore";

  public static final String METASTORE_CLIENT_TRANSPORT_MODE_KEY = "hive.metastore.client.thrift.transport.mode";
  public static final String METASTORE_CLIENT_TRANSPORT_MODE_DEFAULT = "http";

  public static final String METASTORE_CLIENT_HTTP_PATH_KEY = "metastore.client.thrift.http.path";
  public static final String METASTORE_CLIENT_HTTP_PATH_DEFAULT = "metastore";

  public static final String METASTORE_SERVER_MAX_THREADS_KEY = "metastore.server.max.threads";
  public static final String METASTORE_SERVER_MAX_THREADS_HIVE_KEY = "hive.metastore.server.max.threads";
  public static final int METASTORE_SERVER_MAX_THREADS_DEFAULT = 1000;

  public static final String HIVE_METASTORE_URIS_KEY = "hive.metastore.uris";

  public static final String HIVE_SERVER2_TEZ_SESSIONS_PER_QUEUE_KEY = "hive.server2.tez.sessions.per.default.queue";
  public static final int HIVE_SERVER2_TEZ_SESSIONS_PER_QUEUE_DEFAULT = 1;

  public static final String HIVE_SERVER2_THRIFT_PORT_KEY = "hive.server2.thrift.port";
  public static final int HIVE_SERVER2_THRIFT_PORT_DEFAULT = 10000;

  public static final String HIVE_SERVER2_THRIFT_HTTP_PORT_KEY = "hive.server2.thrift.http.port";
  public static final int HIVE_SERVER2_THRIFT_HTTP_PORT_DEFAULT = 10001;

  public static final String HIVE_SERVER2_THRIFT_HTTP_PATH_KEY = "hive.server2.thrift.http.path";
  public static final String HIVE_SERVER2_THRIFT_HTTP_PATH_DEFAULT = "cliservice";

  public static final String HIVE_SERVER2_TRANSPORT_MODE_KEY = "hive.server2.transport.mode";
  public static final String HIVE_SERVER2_TRANSPORT_MODE_DEFAULT = "http";

  public static final String HIVE_SERVER2_WEBUI_PORT_KEY = "hive.server2.webui.port";
  public static final int HIVE_SERVER2_WEBUI_PORT_DEFAULT = 10002;

  /** Port for the Prometheus JMX Exporter agent (serves /metrics in text format). */
  public static final int PROMETHEUS_JMX_EXPORTER_PORT = 9404;

  /** Default URL for the Prometheus JMX Exporter javaagent JAR. */
  public static final String JMX_EXPORTER_JAR_URL =
      "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar";

  public static final String TEZ_AM_SESSION_MODE_KEY = "tez.am.mode.session";

  public static final String TEZ_IGNORE_LIB_URIS_KEY = "tez.ignore.lib.uris";

  public static final String TEZ_AM_WEBSERVICE_ENABLE_KEY = "tez.am.webservice.enable";

  public static final String TEZ_AM_DISABLE_CLIENT_VERSION_CHECK_KEY = "tez.am.disable.client-version-check";

  public static final String TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS_KEY = "tez.session.am.dag.submit.timeout.secs";

  public static final String TEZ_LOCAL_MODE_KEY = "tez.local.mode";

  /** tez.am.framework.mode - only available in Tez 1.0.0+ */
  public static final String TEZ_AM_FRAMEWORK_MODE_KEY = "tez.am.framework.mode";

  /** tez.am.registry.namespace - only available in Tez 1.0.0+ */
  public static final String TEZ_AM_REGISTRY_NAMESPACE_KEY = "tez.am.registry.namespace";

  /** tez.am.zookeeper.quorum - only available in Tez 1.0.0+ */
  public static final String TEZ_AM_ZOOKEEPER_QUORUM_KEY = "tez.am.zookeeper.quorum";

  public static int getInt(Map<String, String> overrides,
      String key, String altKey, int defaultVal) {
    if (overrides != null) {
      String val = overrides.get(key);
      if (val == null && altKey != null) {
        val = overrides.get(altKey);
      }
      if (val != null) {
        return Integer.parseInt(val);
      }
    }
    return defaultVal;
  }
}
