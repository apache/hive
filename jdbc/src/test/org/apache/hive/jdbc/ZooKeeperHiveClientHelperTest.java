/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetDataBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class ZooKeeperHiveClientHelperTest {

  private Utils.JdbcConnectionParams connParams;
  private CuratorFramework mockZooKeeperClient;

  @Mock
  private GetDataBuilder mockGetDataBuilder;

  private final String input;
  private final String expectedHost;
  private final int expectedPort;
  private final boolean expectException;

  public ZooKeeperHiveClientHelperTest(String input, String expectedHost, int expectedPort, boolean expectException) {
    this.input = input;
    this.expectedHost = expectedHost;
    this.expectedPort = expectedPort;
    this.expectException = expectException;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        // Valid cases
        {"localhost:9090", "localhost", 9090, false},        // Hostname
        {"192.168.1.1:8080", "192.168.1.1", 8080, false},    // IPv4
        {"[2001:db8::1]:1234", "2001:db8::1", 1234, false},  // IPv6 with square brackets
        {"2001:db8::1:5678", "2001:db8::1", 5678, false},    // IPv6 without square brackets

        // Invalid cases (Invalid host and port format)
        {"localhost", null, 0, true},         // Missing port
        {":9090", null, 0, true},             // Missing hostname
        {"192.168.1.1:", null, 0, true},      // Missing port after IPv4
        {"[2001:db8::1]", null, 0, true},     // Missing port after IPv6 with square brackets
        {"invalid_host:port", null, 0, true}  // Invalid host
    });
  }

  @Before
  public void setUp() {
    connParams = new Utils.JdbcConnectionParams();
    mockZooKeeperClient = mock(CuratorFramework.class);
    mockGetDataBuilder = mock(GetDataBuilder.class);

    // Mock the getData() method to return mockGetDataBuilder
    when(mockZooKeeperClient.getData()).thenReturn(mockGetDataBuilder);
  }

  @Test
  public void testUpdateParamsWithZKServerNode_HostPortParsing_Success() throws Exception {
    String serverNode = "testServerNode";

    // Mock the behavior of getData().forPath() to return the data bytes
    when(mockGetDataBuilder.forPath(anyString())).thenReturn(input.getBytes(StandardCharsets.UTF_8));

    // Access the private method using reflection
    Method updateParamsMethod = ZooKeeperHiveClientHelper.class.getDeclaredMethod(
        "updateParamsWithZKServerNode", Utils.JdbcConnectionParams.class, CuratorFramework.class, String.class);

    // Make the private method accessible
    updateParamsMethod.setAccessible(true);
    
    try {
      // Call the private method under test
      updateParamsMethod.invoke(null, connParams, mockZooKeeperClient, serverNode);

      if (expectException) {
        fail("Expected IllegalArgumentException or ZooKeeperHiveClientException due to invalid format.");
      } else {
        assertEquals(connParams.getHost(), expectedHost);
        assertEquals(connParams.getPort(), expectedPort);
      }
    } catch (Exception e) {
      // We expect exceptions for invalid input, so the test passes if we catch them
      assertTrue(expectException && 
          e.getCause() instanceof IllegalArgumentException || e.getCause() instanceof ZooKeeperHiveClientException);
    }
  }
}