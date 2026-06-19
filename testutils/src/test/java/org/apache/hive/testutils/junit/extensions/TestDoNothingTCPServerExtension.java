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
package org.apache.hive.testutils.junit.extensions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;

/**
 * Tests for {@link DoNothingTCPServerExtension}.
 */
public class TestDoNothingTCPServerExtension {

  @Test
  @ExtendWith(DoNothingTCPServerExtension.class)
  void testSingleServerListening(DoNothingTCPServer server) throws IOException {
    try (Socket socket = new Socket()) {
      socket.connect(new InetSocketAddress("localhost", server.port()), 1000);
    }
  }

  @Test
  @ExtendWith(DoNothingTCPServerExtension.class)
  void testMultipleServersListening(DoNothingTCPServer s1, DoNothingTCPServer s2) throws IOException {
    for (DoNothingTCPServer s : Arrays.asList(s1, s2)) {
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress("localhost", s.port()), 1000);
      }
    }
  }

  @Test
  @ExtendWith(DoNothingTCPServerExtension.class)
  void testMultipleServersDistinct(DoNothingTCPServer s1, DoNothingTCPServer s2) {
    Assertions.assertNotEquals(s1, s2);
    Assertions.assertNotEquals(s1.port(), s2.port());
  }
}
