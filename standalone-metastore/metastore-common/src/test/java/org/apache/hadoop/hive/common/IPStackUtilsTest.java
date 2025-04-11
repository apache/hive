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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IPStackUtilsTest {

  @Test
  void testIPv4LoopbackWhenIPv4StackIsForced() {
    IPStackUtils.setPreferIPv4Stack(true);
    IPStackUtils.setPreferIPv6Addresses(false);

    String loopback = IPStackUtils.resolveLoopbackAddress();
    Assertions.assertEquals(IPStackUtils.LOOPBACK_ADDRESS_IPV4, loopback);
  }

  @Test
  void testIPv6LoopbackWhenIPv6IsPreferred() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(true);

    String loopback = IPStackUtils.resolveLoopbackAddress();
    Assertions.assertEquals(IPStackUtils.LOOPBACK_ADDRESSES_IPV6.get(0), loopback);
  }

  @Test
  void testIPv4LoopbackWhenIPv6IsNotPreferred() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(false);

    String loopback = IPStackUtils.resolveLoopbackAddress();
    Assertions.assertEquals(IPStackUtils.LOOPBACK_ADDRESS_IPV4, loopback);
  }

  @Test
  void testIPv4WildcardWhenIPv4StackIsForced() {
    IPStackUtils.setPreferIPv4Stack(true);
    IPStackUtils.setPreferIPv6Addresses(false);

    String wildcard = IPStackUtils.resolveWildcardAddress();
    Assertions.assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, wildcard);
  }

  @Test
  void testIPv6WildcardWhenIPv6IsPreferred() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(true);

    String wildcard = IPStackUtils.resolveWildcardAddress();
    Assertions.assertEquals(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0), wildcard);
  }

  @Test
  void testIPv4WildcardWhenIPv6IsNotPreferred() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(false);
    
    String wildcard = IPStackUtils.resolveWildcardAddress();
    Assertions.assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, wildcard);
  }

  @Test
  void testConcatHostPort() {
    Assertions.assertEquals("192.168.1.1:8080", IPStackUtils.concatHostPort("192.168.1.1", 8080));
    Assertions.assertEquals("[2001:db8::1]:8080", IPStackUtils.concatHostPort("2001:db8::1", 8080));
    Assertions.assertEquals("[::1]:9090", IPStackUtils.concatHostPort("::1", 9090));
    Assertions.assertEquals("example.com:443", IPStackUtils.concatHostPort("example.com", 443));
  }
  
  @Test
  void testWildcardWhenIPv4StackIsForcedAndIPv4WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(true);
    IPStackUtils.setPreferIPv6Addresses(false);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESS_IPV4);
    Assertions.assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testWildcardWhenIPv4StackIsForcedAndIPv6WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(true);
    IPStackUtils.setPreferIPv6Addresses(false);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0));
    Assertions.assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, result);
  }


  @Test
  void testWildcardWhenIPv6IsPreferredAndIPv6WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(true);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0));
    Assertions.assertEquals(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0), result);
  }

  @Test
  void testWildcardWhenIPv6IsPreferredAndIPv4WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(true);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESS_IPV4);
    Assertions.assertEquals(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0), result);
  }

  @Test
  void testWildcardWhenIPv6IsNotPreferredAndIPv4WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(false);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESS_IPV4);
    Assertions.assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testWildcardWhenIPv6IsNotPreferredAndIPv6WildcardProvided() {
    IPStackUtils.setPreferIPv4Stack(false);
    IPStackUtils.setPreferIPv6Addresses(false);

    String result = IPStackUtils.adaptWildcardAddress(IPStackUtils.WILDCARD_ADDRESSES_IPV6.get(0));
    Assertions.assertEquals(IPStackUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testAdaptWildcardAddress() {
    Assertions.assertEquals("192.168.1.1", IPStackUtils.adaptWildcardAddress("192.168.1.1"));
    Assertions.assertEquals("2001:db8::1", IPStackUtils.adaptWildcardAddress("2001:db8::1"));
    Assertions.assertEquals("example.com", IPStackUtils.adaptWildcardAddress("example.com"));
  }

    // Test cases for getHostAndPort method

  @Test
  void testGetHostAndPortWithIPv4() {
    IPStackUtils.HostPort result = IPStackUtils.getHostAndPort("192.168.1.1:8080");
    Assertions.assertEquals("192.168.1.1", result.getHostname());
    Assertions.assertEquals(8080, result.getPort());
  }

  @Test
  void testGetHostAndPortWithValidIPv6WithSquaredBrackets() {
    IPStackUtils.HostPort result = IPStackUtils.getHostAndPort("[2001:0db8::1]:8080");
    Assertions.assertEquals("2001:0db8::1", result.getHostname());
    Assertions.assertEquals(8080, result.getPort());
  }

  @Test
  void testGetHostAndPortWithValidIPv6WithoutSquaredBrackets() {
    IPStackUtils.HostPort result = IPStackUtils.getHostAndPort("2001:0db8::1:8080");
    Assertions.assertEquals("2001:0db8::1", result.getHostname());
    Assertions.assertEquals(8080, result.getPort());
  }

  @Test
  void testGetHostAndPortWithHostname() {
    IPStackUtils.HostPort result = IPStackUtils.getHostAndPort("example.com:80");
    Assertions.assertEquals("example.com", result.getHostname());
    Assertions.assertEquals(80, result.getPort());
  }

  @Test
  void testGetHostPortWithInvalidAndPort() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getHostAndPort("192.168.1.1:70000"),
        "Port number out of range (0-65535).");
    Assertions.assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getHostAndPort("192.168.1.1"),
        "Input does not contain a port.");
    Assertions.assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getHostAndPort(":8080"),
        "Host address is null or empty.");
  }

  // Test cases for getPort method

  @Test
  void testGetPort() {
    Assertions.assertEquals(8080, IPStackUtils.getPort("8080"));
    Assertions.assertEquals(65535, IPStackUtils.getPort("65535"));
    Assertions.assertEquals(0, IPStackUtils.getPort("0"));
  }

  @Test
  void testGetPortWithInvalidPort() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getPort("70000"),
        "Port number out of range (0-65535).");
    Assertions.assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getPort("-1"),
        "Port number out of range (0-65535).");
    Assertions.assertThrows(IllegalArgumentException.class, () -> IPStackUtils.getPort("abc"),
        "For input string: \"abc\"");
  }
}