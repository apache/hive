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

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.NetUtil;
import org.apache.commons.lang3.StringUtils;

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
    validateHostNotEmpty(host);
    validatePort(port);
    return formatIPAddressForURL(host) + ":" + port;
  }

  public static String concatHostPort(String host, String port) {
    return concatHostPort(host, Integer.parseInt(port));
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

  /**
   * Splits a given input string representing a Hostname or an IP address and port into an `HostPort` object.
   * The input string must be in the format of IPv4/IPv6/[IPv6]/hostname:port.
   * 
   * @param input The input string containing the Hostname/IP address and port, in the format 
   *              "IPv4:port", "[IPv6]:port", "IPv6:port", or "hostname:port".
   * @return A {@link HostPort} object containing the parsed IP address and port number.
   * @throws IllegalArgumentException If the input format is invalid, if the host is null or empty,
   *                                  or if the port number is invalid.
   */
  public static HostPort getHostAndPort(String input) {
    String host;
    int port;

    if (StringUtils.isEmpty(input)) {
      throw new IllegalArgumentException("Input string is null or empty");
    }

    // Check if the input contains a colon, which separates the host and port
    int colonIndex = input.lastIndexOf(':');
    if (colonIndex == -1) {
      throw new IllegalArgumentException("Input does not contain a port.");
    }

    // Extract the host and port parts
    host = input.substring(0, colonIndex);
    port = getPort(input.substring(colonIndex + 1));

    // Check if the host is not null or empty
    validateHostNotEmpty(host);

    // Handle IPv6 addresses enclosed in square brackets (e.g., [IPv6]:port)
    if (host.startsWith("[") && host.endsWith("]")) {
      host = host.substring(1, host.length() - 1); // Remove the square brackets
    }

    return new HostPort(host, port);
  }

  /**
   * Returns an integer representation of the port number.
   * Also validates whether the given string represents a valid port number.
   * A valid port number is an integer between 0 and 65535 inclusive.
   *
   * @param portString The string representing the port number.
   * @return {@code int} the port number.
   */
  public static int getPort(String portString) {
    if (StringUtils.isEmpty(portString)) {
      throw new IllegalArgumentException("port is null or empty");
    }

    int port = Integer.parseInt(portString);
    validatePort(port);
    return port;
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

  public static class HostPort {

    private final String hostname;
    private final int port;

    public HostPort(String hostname, int port) {
      this.hostname = hostname;
      this.port = port;
    }

    public String getHostname() {
      return hostname;
    }

    public int getPort() {
      return port;
    }
  }
}
