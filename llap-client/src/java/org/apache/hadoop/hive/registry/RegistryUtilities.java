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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class RegistryUtilities {
  private static final String LOCALHOST = "localhost";

  /**
   * Will return hostname stored in InetAddress.
   *
   * @return hostname
   */
  public static String getHostName() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return LOCALHOST;
    }
  }

  /**
   * Will return FQDN of the host after doing reverse DNS lookip.
   *
   * @return FQDN of host
   */
  public static String getCanonicalHostName() {
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      return LOCALHOST;
    }
  }

  public static String getUUID() {
    return String.valueOf(UUID.randomUUID());
  }
}