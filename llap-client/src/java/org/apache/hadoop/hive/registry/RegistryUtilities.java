/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.registry;

import java.util.Optional;
import java.util.UUID;

import org.apache.hadoop.hive.common.ServerUtils;

public class RegistryUtilities {

  /**
   * Will return hostname stored in InetAddress.
   *
   * @return hostname
   */
  public static String getHostName() {
    return ServerUtils.hostname(Optional.of("localhost"));
  }

  /**
   * Will return FQDN of the host after doing reverse DNS lookip.
   *
   * @return FQDN of host
   */
  public static String getCanonicalHostName() {
    return ServerUtils.canonicalHostName(Optional.of("localhost"));
  }

  public static String getUUID() {
    return String.valueOf(UUID.randomUUID());
  }
}