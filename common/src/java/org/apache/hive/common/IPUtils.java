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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

public class IPUtils {

  public static final String WILDCARD_ADDRESS_IPV4 = "0.0.0.0";
  public static final String WILDCARD_ADDRESS_IPV6 = "::";

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

  /**
   * Translates the provided wildcard address for the active IP Stack version if it is a wildcard address,
   * otherwise returns the provided address.
   * @param hostname An ip address
   * @return the translated ip address or the provided address
   */
  public static String translateWildcardAddressForActiveStack(String hostname) {
    if (hostname.equals(WILDCARD_ADDRESS_IPV4) || hostname.equals(WILDCARD_ADDRESS_IPV6)) {
      // The provided address is a wildcard address, return the wildcard address for the active IP stack
      return getWildcardAddress();
    } else {
      return hostname;
    }
  }

  /**
   * If the provided address is an IPv4 address, translates it to IPv6, otherwise returns the provided address.
   * @param ip An IPv4 address
   * @return the translated ip address
   */
  public static String translateIPv4IPForActiveStack(String ip) {
    if (isValidIPv4(ip)) {
      try {
        return InetAddress.getByName("::ffff:" + ip).getHostAddress();
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    } else {
      return ip;
    }
  }

  /**
   * Concats the IPv4 or IPv6 wildcard address depending on the preferred stack with the specified port.
   * @return the wildcard address and port string
   */
  public static String concatWildcardAddressPort(int port) {
    String address = getWildcardAddress();
    if (address.equals(WILDCARD_ADDRESS_IPV6)) {
      address = "[" + address + "]";
    }
    return address + ":" + port;
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
   * Get the IPv4 or IPv6 loopback address depending on Java properties.
   * @return the loopback address
   */
  public static String getLoopbackAddress() {
    boolean preferIPv6Addresses = Boolean.getBoolean("java.net.preferIPv6Addresses");
    boolean preferIPv4Stack = Boolean.getBoolean("java.net.preferIPv4Stack");

    if (preferIPv4Stack) {
      // IPv6 stack is completely disabled on Java side
      return Inet4Address.getLoopbackAddress().getHostAddress();
    } else if (preferIPv6Addresses) {
      // Dual stack is enabled, and IPv6 addresses are preferred
      return Inet6Address.getLoopbackAddress().getHostAddress();
    } else {
      // Dual stack is enabled, and IPv6 addresses are not preferred
      return Inet4Address.getLoopbackAddress().getHostAddress();
    }
  }

  /**
   * Concatenates the IPv4 or IPv6 loopback address depending on the preferred stack with the specified port.
   * @return the wildcard address and port string
   */
  public static String concatLoopbackAddressPort(int port) {
    String address = getLoopbackAddress();
    if (address.equals(Inet6Address.getLoopbackAddress().getHostAddress())) {
      address = "[" + address + "]";
    }
    return address + ":" + port;
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
}
