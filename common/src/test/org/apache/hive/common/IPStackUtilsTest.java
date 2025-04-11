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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IPStackUtilsTest {

  @Test
  void testIPv4LoopbackWhenIPv4StackIsForced() {
    IPStackUtils.setPreferIPv4Stack(true);
    IPStackUtils.setPreferIPv6Addresses(false);

    String loopback = IPStackUtils.resolveLoopbackAddress();
    assertEquals(IPStackUtils.LOOPBACK_ADDRESS_IPV4, loopback);
  }

  @Test
  void testIPv6LoopbackWhenIPv6IsPreferred() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(true);

    String loopback = IPStackUtils.resolveLoopbackAddress();
    assertEquals(IPStackUtils.LOOPBACK_ADDRESSES_IPV6.get(0), loopback);
  }

  @Test
  void testIPv4LoopbackWhenIPv6IsNotPreferred() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(false);

    String loopback = IPStackUtils.resolveLoopbackAddress();
    assertEquals(IPStackUtils.LOOPBACK_ADDRESS_IPV4, loopback);
  }

  @Test
  void testIPv4WildcardWhenIPv4StackIsForced() {
    IPStackUtils.setPreferIPv4Stack(true);
    IPStackUtils.setPreferIPv6Addresses(false);

    String wildcard = IPStackUtils.resolveWildcardAddress();
    assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, wildcard);
  }

  @Test
  void testIPv6WildcardWhenIPv6IsPreferred() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(true);

    String wildcard = IPStackUtils.resolveWildcardAddress();
    assertEquals(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0), wildcard);
  }

  @Test
  void testIPv4WildcardWhenIPv6IsNotPreferred() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(false);
    
    String wildcard = IPStackUtils.resolveWildcardAddress();
    assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, wildcard);
  }

  @Test
  void testConcatHostPort() {
    assertEquals("192.168.1.1:8080", IPStackUtils.concatHostPort("192.168.1.1", 8080));
    assertEquals("[2001:db8::1]:8080", IPStackUtils.concatHostPort("2001:db8::1", 8080));
    assertEquals("[::1]:9090", IPStackUtils.concatHostPort("::1", 9090));
    assertEquals("example.com:443", IPStackUtils.concatHostPort("example.com", 443));
  }
  
  @Test
  void testWildcardWhenIPv4StackIsForcedAndIPv4WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(true);
    IPStackUtils.setPreferIPv6Addresses(false);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESS_IPV4);
    assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testWildcardWhenIPv4StackIsForcedAndIPv6WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(true);
    IPStackUtils.setPreferIPv6Addresses(false);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0));
    assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, result);
  }


  @Test
  void testWildcardWhenIPv6IsPreferredAndIPv6WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(true);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0));
    assertEquals(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0), result);
  }

  @Test
  void testWildcardWhenIPv6IsPreferredAndIPv4WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(true);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESS_IPV4);
    assertEquals(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0), result);
  }

  @Test
  void testWildcardWhenIPv6IsNotPreferredAndIPv4WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(false);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESS_IPV4);
    assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testWildcardWhenIPv6IsNotPreferredAndIPv6WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(false);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0));
    assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testAdaptWildcardAddress() {
    assertEquals("192.168.1.1", IPStackUtils.adaptWildcardAddress("192.168.1.1"));
    assertEquals("2001:db8::1", IPStackUtils.adaptWildcardAddress("2001:db8::1"));
    assertEquals("example.com", IPStackUtils.adaptWildcardAddress("example.com"));
  }

    // Test cases for getHostAndPort method

  @Test
  void testGetHostAndPortWithIPv4() {
    IPStackUtils.HostPort result = IPStackUtils.getHostAndPort("192.168.1.1:8080");
    assertEquals("192.168.1.1", result.getHostname());
    assertEquals(8080, result.getPort());
  }

  @Test
  void testGetHostAndPortWithValidIPv6WithSquaredBrackets() {
    IPStackUtils.HostPort result = IPStackUtils.getHostAndPort("[2001:0db8::1]:8080");
    assertEquals("2001:0db8::1", result.getHostname());
    assertEquals(8080, result.getPort());
  }

  @Test
  void testGetHostAndPortWithValidIPv6WithoutSquaredBrackets() {
    IPStackUtils.HostPort result = IPStackUtils.getHostAndPort("2001:0db8::1:8080");
    assertEquals("2001:0db8::1", result.getHostname());
    assertEquals(8080, result.getPort());
  }

  @Test
  void testGetHostAndPortWithHostname() {
    IPStackUtils.HostPort result = IPStackUtils.getHostAndPort("example.com:80");
    assertEquals("example.com", result.getHostname());
    assertEquals(80, result.getPort());
  }

  @Test
  void testGetHostPortWithInvalidAndPort() {
    assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getHostAndPort("192.168.1.1:70000"),
        "Port number out of range (0-65535).");
    assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getHostAndPort("192.168.1.1"),
        "Input does not contain a port.");
    assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getHostAndPort(":8080"),
        "Host address is null or empty.");
  }

  // Test cases for getPort method

  @Test
  void testGetPort() {
    assertEquals(8080, IPStackUtils.getPort("8080"));
    assertEquals(65535, IPStackUtils.getPort("65535"));
    assertEquals(0, IPStackUtils.getPort("0"));
  }

  @Test
  void testGetPortWithInvalidPort() {
    assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getPort("70000"),
        "Port number out of range (0-65535).");
    assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getPort("-1"),
        "Port number out of range (0-65535).");
    assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getPort("abc"),
        "For input string: \"abc\"");
  }
}