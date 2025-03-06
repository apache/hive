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
  public void testValidIPv4Addresses() {
    assertTrue(IPUtils.isValidIPv4("192.168.1.1"));
    assertTrue(IPUtils.isValidIPv4("255.255.255.255"));
    assertTrue(IPUtils.isValidIPv4("0.0.0.0"));
    assertTrue(IPUtils.isValidIPv4("1.1.1.1"));
  }

  @Test
  public void testInvalidIPv4Addresses() {
    assertFalse(IPUtils.isValidIPv4("256.100.50.25")); // Invalid: 256 out of range
    assertFalse(IPUtils.isValidIPv4("192.168.1")); // Invalid: Missing octet
    assertFalse(IPUtils.isValidIPv4("192.168.1.01")); // Invalid: Leading zero
    assertFalse(IPUtils.isValidIPv4("192.168.1.1.1")); // Invalid: Extra octet
    assertFalse(IPUtils.isValidIPv4("abcd")); // Invalid: Not an IP
    assertFalse(IPUtils.isValidIPv4("1234.123.123.123")); // Invalid: Octet out of range
  }

  @Test
  public void testValidIPv6Addresses() {
    assertTrue(IPUtils.isValidIPv6("2001:db8::ff00:42:8329")); // Valid IPv6
    assertTrue(IPUtils.isValidIPv6("::1")); // Loopback address
    assertTrue(IPUtils.isValidIPv6("2001:0db8:85a3:0000:0000:8a2e:0370:7334")); // Full notation
    assertTrue(IPUtils.isValidIPv6("fe80::1")); // Link-local address
    assertTrue(IPUtils.isValidIPv6("::")); // Unspecified address
  }

  @Test
  public void testInvalidIPv6Addresses() {
    assertFalse(IPUtils.isValidIPv6("192.168.1.1")); // IPv4 address
    assertFalse(IPUtils.isValidIPv6("abcd")); // Not an IP
    assertFalse(IPUtils.isValidIPv6("2001:db8:::1")); // Too many colons
    assertFalse(IPUtils.isValidIPv6("2001:db8::gggg")); // Invalid hex characters
    assertFalse(IPUtils.isValidIPv6("2001:db8::1::1")); // Multiple "::"
  }
}