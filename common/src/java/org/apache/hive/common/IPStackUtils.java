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

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.NetUtil;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class IPStackUtils {

  public static final String WILDCARD_ADDRESS_IPV4 = "0.0.0.0";
  public static final String LOOPBACK_ADDRESS_IPV4 = "127.0.0.1";

  public static final List<String> WILDCARD_ADDRESSES_IPV6 = Collections.unmodifiableList(
      Arrays.asList("::", "0:0:0:0:0:0:0:0"));
  public static final List<String> LOOPBACK_ADDRESSES_IPV6 = Collections.unmodifiableList(
      Arrays.asList("::1", "0:0:0:0:0:0:0:1"));

  private static boolean preferIPv4Stack =  NetUtil.isIpV4StackPreferred();
  private static boolean preferIPv6Addresses = NetUtil.isIpV6AddressesPreferred();

  private IPStackUtils() {
  }

  @VisibleForTesting
  static void setPreferIPv4Stack(boolean preferIPv4Stack) {
    IPStackUtils.preferIPv4Stack = preferIPv4Stack;
  }

  @VisibleForTesting
  static void setPreferIPv6Addresses(boolean preferIPv6Addresses) {
    IPStackUtils.preferIPv6Addresses = preferIPv6Addresses;
  }
  
  /**
   * Get the IPv4 or IPv6 wildcard address for binding on all network interfaces,
   * depending on Java properties.
   * @return the wildcard address
   */
  public static String resolveWildcardAddress() {
    if (preferIPv4Stack) {
      // IPv6 stack is completely disabled on Java side
      return WILDCARD_ADDRESS_IPV4;
    } else if (preferIPv6Addresses) {
      // Dual stack is enabled, and IPv6 addresses are preferred
      return WILDCARD_ADDRESSES_IPV6.get(0);
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
    return concatHostPort(resolveWildcardAddress(), port);
  }

  /**
   * Adapts provided wildcard address for the active IP Stack. If the provided is IPv4 wildcard address, and the
   * active stack is IPv6, returns IPv6 wildcard address, and vice versa. If the provided address is not a wildcard 
   * address, returns back provided address. 
   * @param hostname An ip address or hostname
   * @return the updated wildcard address or the provided address
   */
  public static String adaptWildcardAddress(String hostname) {
    if (WILDCARD_ADDRESS_IPV4.equals(hostname) || WILDCARD_ADDRESSES_IPV6.contains(hostname)) {
      // The provided address is a wildcard address, return the wildcard address for the active IP stack
      return resolveWildcardAddress();
    } else {
      return hostname;
    }
  }

  /**
   * Adapts provided loopback address for the active IP Stack. If the provided is IPv4 loopback address, and the
   * active stack is IPv6, returns IPv6 loopback address, and vice versa. If the provided address is not a loopback
   * address, returns back provided address.
   * @param hostname An ip address or hostname
   * @return the updated wildcard address or the provided address
   */
  public static String adaptLoopbackAddress(String hostname) {
    if (LOOPBACK_ADDRESS_IPV4.equals(hostname) || LOOPBACK_ADDRESSES_IPV6.contains(hostname)) {
      // The provided address is a loopback address, return the loopback address for the active IP stack
      return resolveLoopbackAddress();
    } else {
      return hostname;
    }
  }
  
  /**
   * Get the IPv4 or IPv6 loopback address depending on Java properties.
   * @return the loopback address
   */
  public static String resolveLoopbackAddress() {
    if (preferIPv4Stack) {
      // IPv6 stack is completely disabled on Java side
      return LOOPBACK_ADDRESS_IPV4;
    } else if (preferIPv6Addresses) {
      // Dual stack is enabled, and IPv6 addresses are preferred
      return LOOPBACK_ADDRESSES_IPV6.get(0);
    } else {
      // Dual stack is enabled, and IPv6 addresses are not preferred
      return LOOPBACK_ADDRESS_IPV4;
    }
  }

  /**
   * Check if the provided IP address is a loopback interface for the active IP stack.
   * @return boolean
   */
  public static boolean isActiveStackLoopbackIP(String ipAddress) {
    if (preferIPv4Stack) {
      // IPv6 stack is completely disabled on Java side
      return LOOPBACK_ADDRESS_IPV4.equals(ipAddress);
    } else if (preferIPv6Addresses) {
      // Dual stack is enabled, and IPv6 addresses are preferred
      return LOOPBACK_ADDRESSES_IPV6.contains(ipAddress);
    } else {
      // Dual stack is enabled, and IPv6 addresses are not preferred
      return LOOPBACK_ADDRESS_IPV4.equals(ipAddress);
    }
  }

  /**
   * Concatenates the IPv4 or IPv6 loopback address depending on the preferred stack with the specified port.
   * @return the wildcard address and port string
   */
  public static String concatLoopbackAddressPort(int port) {
    return concatHostPort(resolveLoopbackAddress(), port);
  }
  
  /** 
   * Concatenates the host and port with a colon. 
   * If the host is an IPv6 address, it is enclosed in square brackets.
   * @param host the host
   * @param port the port
   * @return the concatenated host and port
   */
  public static String concatHostPort(String host, int port) {
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

  /**
   * If the provided address is an IPv4 address, transforms it to IPv6, otherwise returns the provided address.
   * Used in some tests which use hardcoded IPv4 addresses that need to be IPv6 when active stack is IPv6.
   * @param ipv4 An IPv4 address
   * @return the transformed IPv4 address
   */
  public static String transformToIPv6(String ipv4) {
    if (NetUtil.isValidIpV4Address(ipv4)) {
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
    if (NetUtil.isValidIpV4Address(ipv4)) {
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
