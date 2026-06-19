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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;

/**
 * Configuration for LLAP daemon processes only. This should not be used by any clients.
 */
@InterfaceAudience.Private
public class LlapDaemonConfiguration extends Configuration {

  @InterfaceAudience.Private
  public static final String LLAP_DAEMON_SITE = "llap-daemon-site.xml";

  @InterfaceAudience.Private
  public static final String[] DAEMON_CONFIGS = { /* in specific order */"core-site.xml",
      "hdfs-site.xml", "yarn-site.xml", "tez-site.xml", "hive-site.xml" };

  @InterfaceAudience.Private
  public static final String[] SSL_DAEMON_CONFIGS = { "ssl-client.xml" };

  public LlapDaemonConfiguration() {
    super(true); // Load the defaults.
    for (String conf : DAEMON_CONFIGS) {
      addResource(conf);
    }
    addResource(LLAP_DAEMON_SITE);
  }

  @VisibleForTesting
  public LlapDaemonConfiguration(Configuration conf) {
    this();
    addResource(conf);
  }
}