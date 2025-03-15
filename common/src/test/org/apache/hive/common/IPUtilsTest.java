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

class IPUtilsTest {

  @Test
  void testValidIPv4Addresses() {
    assertTrue(IPUtils.isValidIPv4("192.168.1.1"));
    assertTrue(IPUtils.isValidIPv4("255.255.255.255"));
    assertTrue(IPUtils.isValidIPv4("0.0.0.0"));
    assertTrue(IPUtils.isValidIPv4("1.1.1.1"));
  }

  @Test
  void testInvalidIPv4Addresses() {
    assertFalse(IPUtils.isValidIPv4("256.100.50.25")); // Invalid: 256 out of range
    assertFalse(IPUtils.isValidIPv4("192.168.1")); // Invalid: Missing octet
    assertFalse(IPUtils.isValidIPv4("192.168.1.01")); // Invalid: Leading zero
    assertFalse(IPUtils.isValidIPv4("192.168.1.1.1")); // Invalid: Extra octet
    assertFalse(IPUtils.isValidIPv4("abcd")); // Invalid: Not an IP
    assertFalse(IPUtils.isValidIPv4("1234.123.123.123")); // Invalid: Octet out of range
  }

  @Test
  void testValidIPv6Addresses() {
    assertTrue(IPUtils.isValidIPv6("2001:db8::ff00:42:8329")); // Valid IPv6
    assertTrue(IPUtils.isValidIPv6("::1")); // Loopback address
    assertTrue(IPUtils.isValidIPv6("2001:0db8:85a3:0000:0000:8a2e:0370:7334")); // Full notation
    assertTrue(IPUtils.isValidIPv6("fe80::1")); // Link-local address
    assertTrue(IPUtils.isValidIPv6("::")); // Unspecified address
  }

  @Test
  void testInvalidIPv6Addresses() {
    assertFalse(IPUtils.isValidIPv6("192.168.1.1")); // IPv4 address
    assertFalse(IPUtils.isValidIPv6("abcd")); // Not an IP
    assertFalse(IPUtils.isValidIPv6("2001:db8:::1")); // Too many colons
    assertFalse(IPUtils.isValidIPv6("2001:db8::gggg")); // Invalid hex characters
    assertFalse(IPUtils.isValidIPv6("2001:db8::1::1")); // Multiple "::"
  }

  @Test
  void testIPv4LoopbackWhenIPv4StackIsForced() {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("java.net.preferIPv6Addresses", "false");

    String loopback = IPUtils.getLoopbackAddress();
    assertEquals(IPUtils.LOOPBACK_ADDRESS_IPV4, loopback);
  }

  @Test
  void testIPv6LoopbackWhenIPv6IsPreferred() {
    System.setProperty("java.net.preferIPv4Stack", "false");
    System.setProperty("java.net.preferIPv6Addresses", "true");

    String loopback = IPUtils.getLoopbackAddress();
    assertEquals(IPUtils.LOOPBACK_ADDRESS_IPV6, loopback);
  }

  @Test
  void testIPv4LoopbackWhenIPv6IsNotPreferred() {
    System.setProperty("java.net.preferIPv4Stack", "false");
    System.setProperty("java.net.preferIPv6Addresses", "false");

    String loopback = IPUtils.getLoopbackAddress();
    assertEquals(IPUtils.LOOPBACK_ADDRESS_IPV4, loopback);
  }

  @Test
  void testIPv4WildcardWhenIPv4StackIsForced() {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("java.net.preferIPv6Addresses", "false");

    String wildcard = IPUtils.getWildcardAddress();
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV4, wildcard);
  }

  @Test
  void testIPv6WildcardWhenIPv6IsPreferred() {
    System.setProperty("java.net.preferIPv4Stack", "false");
    System.setProperty("java.net.preferIPv6Addresses", "true");

    String wildcard = IPUtils.getWildcardAddress();
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV6, wildcard);
  }

  @Test
  void testIPv4WildcardWhenIPv6IsNotPreferred() {
    System.setProperty("java.net.preferIPv4Stack", "false");
    System.setProperty("java.net.preferIPv6Addresses", "false");

    String wildcard = IPUtils.getWildcardAddress();
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV4, wildcard);
  }

  @Test
  void testConcatHostPortIPv4Host() {
    assertEquals("192.168.1.1:8080", IPUtils.concatHostPort("192.168.1.1", 8080));
  }

  @Test
  void testConcatHostPortIPv6Host() {
    assertEquals("[2001:db8::1]:8080", IPUtils.concatHostPort("2001:db8::1", 8080));
  }

  @Test
  void testConcatHostPortIPv6Loopback() {
    assertEquals("[::1]:9090", IPUtils.concatHostPort("::1", 9090));
  }

  @Test
  void testConcatHostPortHostname() {
    assertEquals("example.com:443", IPUtils.concatHostPort("example.com", 443));
  }

  @Test
  void testConcatHostPortLoobackIPv4() {
    assertEquals("127.0.0.1:3306", IPUtils.concatHostPort("127.0.0.1", 3306));
  }

  @Test
  void testConcatHostPortLoopbackIPv6() {
    assertEquals("[::1]:3306", IPUtils.concatHostPort("::1", 3306));
  }
  
  @Test
  void testWildcardWhenIPv4StackIsForcedAndIPv4WildcardProvided() {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("java.net.preferIPv6Addresses", "false");

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV4);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testWildcardWhenIPv4StackIsForcedAndIPv6WildcardProvided() {
    System.setProperty("java.net.preferIPv4Stack", "true");
    System.setProperty("java.net.preferIPv6Addresses", "false");

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV6);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV4, result);
  }


  @Test
  void testWildcardWhenIPv6IsPreferredAndIPv6WildcardProvided() {
    System.setProperty("java.net.preferIPv4Stack", "false");
    System.setProperty("java.net.preferIPv6Addresses", "true");

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV6);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV6, result);
  }

  @Test
  void testWildcardWhenIPv6IsPreferredAndIPv4WildcardProvided() {
    System.setProperty("java.net.preferIPv4Stack", "false");
    System.setProperty("java.net.preferIPv6Addresses", "true");

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV4);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV6, result);
  }

  @Test
  void testWildcardWhenIPv6IsNotPreferredAndIPv4WildcardProvided() {
    System.setProperty("java.net.preferIPv4Stack", "false");
    System.setProperty("java.net.preferIPv6Addresses", "false");

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV4);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testWildcardWhenIPv6IsNotPreferredAndIPv6WildcardProvided() {
    System.setProperty("java.net.preferIPv4Stack", "false");
    System.setProperty("java.net.preferIPv6Addresses", "false");

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV6);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testWildcardWhenNonWildcardIPv4AddressProvided() {
    String result = IPUtils.updateWildcardAddress("192.168.1.1");
    assertEquals("192.168.1.1", result);
  }

  @Test
  void testWildcardWhenNonWildcardIPv6AddressProvided() {
    String result = IPUtils.updateWildcardAddress("2001:db8::1");
    assertEquals("2001:db8::1", result);
  }

  @Test
  void testWildcardWhenHostnameIsProvided() {
    String result = IPUtils.updateWildcardAddress("example.com");
    assertEquals("example.com", result);
  }
}