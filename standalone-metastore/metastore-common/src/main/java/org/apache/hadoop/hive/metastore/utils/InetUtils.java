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

package org.apache.hadoop.hive.metastore.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Optional;

/**
 * ServerUtils (specific to HiveServer version 1)
 */
public class InetUtils {

  /**
   * @return name of current host
   */
  public static String hostname() {
    return hostname(Optional.empty());
  }

  /**
   * @return name of current host
   */
  public static String hostname(Optional<String> defaultValue) {
    Objects.requireNonNull(defaultValue);
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return defaultValue.orElseThrow(() -> new RuntimeException("Unable to resolve my host name", e));
    }
  }

  /**
   * @return canonical name of current host
   */
  public static String canonicalHostName(Optional<String> defaultValue) {
    Objects.requireNonNull(defaultValue);
    try {
      return InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      return defaultValue.orElseThrow(() -> new RuntimeException("Unable to resolve my host name", e));
    }
  }

  /**
   * @return address of current address
   */
  public static String hostAddress() {
    try {
      return InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Unable to resolve my host address", e);
    }
  }

}
