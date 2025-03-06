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

package org.apache.hive.common;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

public class IPUtils {

  public static final String WILDCARD_ADDRESS_IPV4 = "0.0.0.0";
  public static final String WILDCARD_ADDRESS_IPV6 = "::";
  public static final String LOOPBACK_ADDRESS_IPV4 = "127.0.0.1";
  public static final String LOOPBACK_ADDRESS_IPV6 = "::1";

  private static final String IPV6_PATTERN =
      "([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|" +  // 1:2:3:4:5:6:7:8
          "([0-9a-fA-F]{1,4}:){1,7}:|" +                  // 1::  or 1:2:3::8
          "([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|" +
          "([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|" +
          "([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|" +
          "([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|" +
          "([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|" +
          "[0-9a-fA-F]{1,4}:(:[0-9a-fA-F]{1,4}){1,6}|" +
          ":((:[0-9a-fA-F]{1,4}){1,7}|:)|" +               // :: or ::1 or ::1:2
          "fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|" + // Link-local
          "::(ffff(:0{1,4}){0,1}:){0,1}" +
          "((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}" +
          "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|" +  // IPv4-mapped (::ffff:192.168.1.1)
          "([0-9a-fA-F]{1,4}:){1,4}:" +
          "((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}" +
          "(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])";

  private static final String IPV4_PATTERN =
      "^(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\\." +
          "(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\\." +
          "(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\\." +
          "(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])$";

  private static final Pattern ipv6Pattern = Pattern.compile("^(" + IPV6_PATTERN + ")$");
  private static final Pattern ipv4Pattern = Pattern.compile(IPV4_PATTERN);

  private IPUtils() {
  }
  
  /**
   * Get the IPv4 or IPv6 wildcard address for binding on all network interfaces,
   * depending on Java properties.
   * @return the wildcard address
   */
  public static String getWildcardAddress() {
    boolean preferIPv6Addresses = Boolean.getBoolean("java.net.preferIPv6Addresses");
    boolean preferIPv4Stack = Boolean.getBoolean("java.net.preferIPv4Stack");

    if (preferIPv4Stack) {
      // IPv6 stack is completely disabled on Java side
      return WILDCARD_ADDRESS_IPV4;
    } else if (preferIPv6Addresses) {
      // Dual stack is enabled, and IPv6 addresses are preferred
      return WILDCARD_ADDRESS_IPV6;
    } else {
      // Dual stack is enabled, and IPv6 addresses are not preferred
      return WILDCARD_ADDRESS_IPV4;
    }
  }

  /**
   * Concats the IPv4 or IPv6 wildcard address depending on the preferred stack with the specified port.
   * @return the wildcard address and port string
   */
  public static String concatWildcardAddressPort(int port) {
    return concatHostPort(getWildcardAddress(), port);
  }

  /**
   * Updates provided wildcard address for the active IP Stack. If the provided is IPv4 wildcard address, and the 
   * active stack is IPv6, returns IPv6 wildcard address, and vice versa. If the provided address is not a wildcard 
   * address, returns back provided address. 
   * @param hostname An ip address or hostname
   * @return the updated wildcard address or the provided address
   */
  public static String updateWildcardAddress(String hostname) {
    if (hostname.equals(WILDCARD_ADDRESS_IPV4) || hostname.equals(WILDCARD_ADDRESS_IPV6)) {
      // The provided address is a wildcard address, return the wildcard address for the active IP stack
      return getWildcardAddress();
    } else {
      return hostname;
    }
  }
  
  /**
   * Get the IPv4 or IPv6 loopback address depending on Java properties.
   * @return the loopback address
   */
  public static String getLoopbackAddress() {
    boolean preferIPv6Addresses = Boolean.getBoolean("java.net.preferIPv6Addresses");
    boolean preferIPv4Stack = Boolean.getBoolean("java.net.preferIPv4Stack");

    if (preferIPv4Stack) {
      // IPv6 stack is completely disabled on Java side
      return LOOPBACK_ADDRESS_IPV4;
    } else if (preferIPv6Addresses) {
      // Dual stack is enabled, and IPv6 addresses are preferred
      return LOOPBACK_ADDRESS_IPV6;
    } else {
      // Dual stack is enabled, and IPv6 addresses are not preferred
      return LOOPBACK_ADDRESS_IPV4;
    }
  }

  /**
   * Concatenates the IPv4 or IPv6 loopback address depending on the preferred stack with the specified port.
   * @return the wildcard address and port string
   */
  public static String concatLoopbackAddressPort(int port) {
    return concatHostPort(getLoopbackAddress(), port);
  }

  /**
   * Checks if the provided ip address is a valid IPv4 address.
   * @return true if the provided ip address is a valid IPv4 address else false
   */
  public static boolean isValidIPv4(String ip) {
    return ipv4Pattern.matcher(ip).matches();
  }

  /**
   * Checks if the provided ip address is a valid IPv6 address.
   * @return true if the provided ip address is a valid IPv6 address else false
   */
  public static boolean isValidIPv6(String ip) {
    return ipv6Pattern.matcher(ip).matches();
  }
  
  /** 
   * Concatenates the host and port with a colon. 
   * If the host is an IPv6 address, it is enclosed in square brackets.
   * @param host the host
   * @param port the port
   * @return the concatenated host and port
   */
  public static String concatHostPort(String host, int port) {
    if (host.contains(":")) {
      // IPv6 address
      return "[" + host + "]:" + port;
    } else {
      // IPv4 address or hostname
      return host + ":" + port;
    }
  }

  /**
   * If the provided address is an IPv4 address, transforms it to IPv6, otherwise returns the provided address.
   * Used in some tests which use hardcoded IPv4 addresses that need to be IPv6 when active stack is IPv6.
   * @param ipv4 An IPv4 address
   * @return the transformed IPv4 address
   */
  public static String transformToIPv6(String ipv4) {
    if (isValidIPv4(ipv4)) {
      try {
        return InetAddress.getByName("::ffff:" + ipv4).getHostAddress();
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    } else {
      return ipv4;
    }
  }

  /**
   * Concatenates the host transformed to IPv6 and port with a colon.
   * Used in some tests which use hardcoded IPv4 addresses and ports that need to be IPv6 when active stack is IPv6.
   * @param ipv4 An IPv4 address
   * @param port port
   * @return the concatenated and transformed to IPv6 host and port
   */
  public static String transformToIPv6(String ipv4, int port) {
    if (isValidIPv4(ipv4)) {
      try {
        return concatHostPort(InetAddress.getByName("::ffff:" + ipv4).getHostAddress(), port);
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    } else {
      return ipv4;
    }
  }
}
