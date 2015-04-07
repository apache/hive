/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.configuration;

import org.apache.hadoop.conf.Configuration;

public class LlapConfiguration extends Configuration {

  public LlapConfiguration(Configuration conf) {
    super(conf);
    addResource(LLAP_DAEMON_SITE);
  }

  public LlapConfiguration() {
    super(false);
    addResource(LLAP_DAEMON_SITE);
  }


  public static final String LLAP_DAEMON_PREFIX = "llap.daemon.";
  private static final String LLAP_DAEMON_SITE = "llap-daemon-site.xml";



  public static final String LLAP_DAEMON_RPC_NUM_HANDLERS = LLAP_DAEMON_PREFIX + "rpc.num.handlers";
  public static final int LLAP_DAEMON_RPC_NUM_HANDLERS_DEFAULT = 5;

  public static final String LLAP_DAEMON_WORK_DIRS = LLAP_DAEMON_PREFIX + "work.dirs";

  public static final String LLAP_DAEMON_YARN_SHUFFLE_PORT = LLAP_DAEMON_PREFIX + "yarn.shuffle.port";
  public static final int LLAP_DAEMON_YARN_SHUFFLE_PORT_DEFAULT = 15551;

  public static final String LLAP_DAEMON_SHUFFLE_DIR_WATCHER_ENABLED = LLAP_DAEMON_PREFIX + "shuffle.dir-watcher.enabled";
  public static final boolean LLAP_DAEMON_SHUFFLE_DIR_WATCHER_ENABLED_DEFAULT = false;

  public static final String LLAP_DAEMON_LIVENESS_HEARTBEAT_INTERVAL_MS = LLAP_DAEMON_PREFIX + "liveness.heartbeat.interval-ms";
  public static final long LLAP_DAEMON_LIVENESS_HEARTBEAT_INTERVAL_MS_DEFAULT = 5000l;


  // Section for configs used in AM and executors
  public static final String LLAP_DAEMON_NUM_EXECUTORS = LLAP_DAEMON_PREFIX + "num.executors";
  public static final int LLAP_DAEMON_NUM_EXECUTORS_DEFAULT = 4;

  public static final String LLAP_DAEMON_RPC_PORT = LLAP_DAEMON_PREFIX + "rpc.port";
  public static final int LLAP_DAEMON_RPC_PORT_DEFAULT = 15001;

  public static final String LLAP_DAEMON_MEMORY_PER_INSTANCE_MB = LLAP_DAEMON_PREFIX + "memory.per.instance.mb";
  public static final int LLAP_DAEMON_MEMORY_PER_INSTANCE_MB_DEFAULT = 4096;

  public static final String LLAP_DAEMON_VCPUS_PER_INSTANCE = LLAP_DAEMON_PREFIX + "vcpus.per.instance";
  public static final int LLAP_DAEMON_VCPUS_PER_INSTANCE_DEFAULT = 4;


  // Section for configs used in the AM //
  public static final String LLAP_DAEMON_SERVICE_HOSTS = LLAP_DAEMON_PREFIX + "service.hosts";

  public static final String LLAP_DAEMON_COMMUNICATOR_NUM_THREADS = LLAP_DAEMON_PREFIX + "communicator.num.threads";
  public static final int LLAP_DAEMON_COMMUNICATOR_NUM_THREADS_DEFAULT = 5;

  /**
   * Time after which a previously disabled node will be re-enabled for scheduling. This may be
   * modified by an exponential back-off if failures persist
   */
  public static final String LLAP_DAEMON_TASK_SCHEDULER_NODE_REENABLE_TIMEOUT_MILLIS =
      LLAP_DAEMON_PREFIX + "task.scheduler.node.re-enable.timeout.ms";
  public static final long LLAP_DAEMON_TASK_SCHEDULER_NODE_REENABLE_TIMEOUT_MILLIS_DEFAULT = 2000l;


}
