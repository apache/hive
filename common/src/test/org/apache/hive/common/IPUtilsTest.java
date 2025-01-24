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

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

class IPUtilsTest {

  @Test
  void testIPv4LoopbackWhenIPv4StackIsForced() {
    IPUtils.setPreferIPv4Stack(true);
    IPUtils.setPreferIPv6Addresses(false);

    String loopback = IPUtils.getLoopbackAddress();
    assertEquals(IPUtils.LOOPBACK_ADDRESS_IPV4, loopback);
  }

  @Test
  void testIPv6LoopbackWhenIPv6IsPreferred() {
    IPUtils.setPreferIPv4Stack(false);
    IPUtils.setPreferIPv6Addresses(true);

    String loopback = IPUtils.getLoopbackAddress();
    assertEquals(IPUtils.LOOPBACK_ADDRESS_IPV6, loopback);
  }

  @Test
  void testIPv4LoopbackWhenIPv6IsNotPreferred() {
    IPUtils.setPreferIPv4Stack(false);
    IPUtils.setPreferIPv6Addresses(false);

    String loopback = IPUtils.getLoopbackAddress();
    assertEquals(IPUtils.LOOPBACK_ADDRESS_IPV4, loopback);
  }

  @Test
  void testIPv4WildcardWhenIPv4StackIsForced() {
    IPUtils.setPreferIPv4Stack(true);
    IPUtils.setPreferIPv6Addresses(false);

    String wildcard = IPUtils.getWildcardAddress();
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV4, wildcard);
  }

  @Test
  void testIPv6WildcardWhenIPv6IsPreferred() {
    IPUtils.setPreferIPv4Stack(false);
    IPUtils.setPreferIPv6Addresses(true);

    String wildcard = IPUtils.getWildcardAddress();
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV6, wildcard);
  }

  @Test
  void testIPv4WildcardWhenIPv6IsNotPreferred() {
    IPUtils.setPreferIPv4Stack(false);
    IPUtils.setPreferIPv6Addresses(false);
    
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
    IPUtils.setPreferIPv4Stack(true);
    IPUtils.setPreferIPv6Addresses(false);

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV4);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testWildcardWhenIPv4StackIsForcedAndIPv6WildcardProvided() {
    IPUtils.setPreferIPv4Stack(true);
    IPUtils.setPreferIPv6Addresses(false);

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV6);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV4, result);
  }


  @Test
  void testWildcardWhenIPv6IsPreferredAndIPv6WildcardProvided() {
    IPUtils.setPreferIPv4Stack(false);
    IPUtils.setPreferIPv6Addresses(true);

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV6);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV6, result);
  }

  @Test
  void testWildcardWhenIPv6IsPreferredAndIPv4WildcardProvided() {
    IPUtils.setPreferIPv4Stack(false);
    IPUtils.setPreferIPv6Addresses(true);

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV4);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV6, result);
  }

  @Test
  void testWildcardWhenIPv6IsNotPreferredAndIPv4WildcardProvided() {
    IPUtils.setPreferIPv4Stack(false);
    IPUtils.setPreferIPv6Addresses(false);

    String result = IPUtils.updateWildcardAddress(IPUtils.WILDCARD_ADDRESS_IPV4);
    assertEquals(IPUtils.WILDCARD_ADDRESS_IPV4, result);
  }

  @Test
  void testWildcardWhenIPv6IsNotPreferredAndIPv6WildcardProvided() {
    IPUtils.setPreferIPv4Stack(false);
    IPUtils.setPreferIPv6Addresses(false);

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