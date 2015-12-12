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

import java.net.URL;

import org.apache.hadoop.conf.Configuration;

public class LlapConfiguration extends Configuration {
  public static final String LLAP_PREFIX = "llap.";
  public static final String LLAP_DAEMON_PREFIX = "llap.daemon.";

  public LlapConfiguration(Configuration conf) {
    super(conf);
    addResource(LLAP_DAEMON_SITE);
  }

  public LlapConfiguration() {
    super(false);
    addResource(LLAP_DAEMON_SITE);
  }

  public LlapConfiguration(Configuration conf, URL llapDaemonConfLocation) {
    super(conf);
    addResource(llapDaemonConfLocation);
  }

  private static final String LLAP_DAEMON_SITE = "llap-daemon-site.xml";
}
