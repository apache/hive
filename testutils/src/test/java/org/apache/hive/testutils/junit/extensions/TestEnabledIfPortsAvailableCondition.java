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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Tests for {@link EnabledIfPortsAvailable}.
 */
public class TestEnabledIfPortsAvailableCondition {

  private static ServerSocket server;

  @BeforeAll
  static void setupSocket2000() {
    try {
      // Best effort to force some tests to be skipped.
      server = new ServerSocket(2000);
    } catch (IOException exception) {
      // It is fine if we don't manage to bind the socket
      // tests should not fail cause of it.
    }
  }

  @AfterAll
  static void tearDownSocket2000() throws IOException {
    if (server != null) {
      server.close();
      server = null;
    }
  }

  @Test
  @EnabledIfPortsAvailable(2000)
  @SuppressWarnings("EmptyTryBlock")
  void testPortMostLikelyInUse() throws IOException {
    // Most of the time the test should be skipped since we are explicitly binding the port with server.
    // If we fail to bind the server to the given port:
    // - due to the port that is already taken; in that case the test should be skipped via the annotation
    // - due to another reason (port is free); in that case the test should pass
    try (@SuppressWarnings("unused") ServerSocket s = new ServerSocket(2000)) {
    }
  }

  @Test
  @EnabledIfPortsAvailable(2001)
  @SuppressWarnings("EmptyTryBlock")
  void testPortMostLikelyAvailableV1() throws IOException {
    // Most of the time the test should pass. In some cases it may happen that the port 2001
    // is bound by somebody else so the test should be skipped.
    try (@SuppressWarnings("unused") ServerSocket s = new ServerSocket(2001)) {
    }
  }

  @Test
  @EnabledIfPortsAvailable(5050)
  @SuppressWarnings("EmptyTryBlock")
  void testPortMostLikelyAvailableV2() throws IOException {
    // Most of the time the test should pass. In some cases it may happen that the port 5050
    // is bound by somebody else so the test should be skipped.
    try (@SuppressWarnings("unused") ServerSocket s = new ServerSocket(5050)) {
    }
  }

}
