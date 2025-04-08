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

package org.apache.hadoop.hive.common;

import org.apache.commons.lang3.StringUtils;

public class IPStackUtils {

  /**
   * Concatenates the host and port with a colon. 
   * If the host is an IPv6 address, it is enclosed in square brackets.
   * @param host the host
   * @param port the port
   * @return the concatenated host and port
   */
  public static String concatHostPort(String host, int port) {
    validateHostNotEmpty(host);
    validatePort(port);
    return formatIPAddressForURL(host) + ":" + port;
  }

  /**
   * Prepares an IP address for use in a URL.
   * <p>
   * This method ensures that IPv6 addresses are enclosed in square brackets,
   * as required by URL syntax. IPv4 addresses and hostnames remain unchanged.
   * </p>
   *
   * @param ipAddress the IP address or hostname to format
   * @return the formatted IP address for use in a URL
   */
  public static String formatIPAddressForURL(String ipAddress) {
    if (ipAddress.contains(":") && !ipAddress.startsWith("[") && !ipAddress.endsWith("]")) {
      // IPv6 address
      return "[" + ipAddress + "]";
    } else {
      // IPv4 address or hostname
      return ipAddress;
    }
  }

  private static void validateHostNotEmpty(String host) {
    if (StringUtils.isEmpty(host) || host.equals("[]")) {
      throw new IllegalArgumentException("Host address is null or empty.");
    }
  }

  private static void validatePort(int port) {
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException("Port number out of range (0-65535).");
    }
  }
}
