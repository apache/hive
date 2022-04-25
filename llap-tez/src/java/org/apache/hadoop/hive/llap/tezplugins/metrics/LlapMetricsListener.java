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

package org.apache.hadoop.hive.llap.tezplugins.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.hadoop.hive.llap.tezplugins.metrics.LlapMetricsCollector.LlapMetrics;

import java.util.Map;

/**
 * Interface to handle Llap Daemon metrics changes.
 */
public interface LlapMetricsListener {

  /**
   * Initializing the listener with the current configuration.
   * @param conf The configuration
   * @param registry The Llap registry service to access the Llap Daemons
   */
  void init(Configuration conf, LlapRegistryService registry);

  /**
   * Handler will be called when new Llap Daemon metrics data is arrived.
   * @param workerIdentity The worker identity of the Llap Daemon
   * @param newMetrics The new metrics object
   */
  void newDaemonMetrics(String workerIdentity, LlapMetrics newMetrics);

  /**
   * Handler will be called when new data is arrived for every active Llap Daemon in the cluster.
   * @param newMetrics The map of the worker indentity -&gt; metrics
   */
  void newClusterMetrics(Map<String, LlapMetrics> newMetrics);
}
