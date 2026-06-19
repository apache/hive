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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A TCP server that accepts a client connection and closes it immediately.
 */
public class DoNothingTCPServer {
  private static final AtomicInteger ID = new AtomicInteger(0);
  private final ServerSocket socket;

  DoNothingTCPServer() throws IOException {
    this.socket = new ServerSocket();
  }

  void start() throws IOException {
    socket.bind(null);
    Thread t = new Thread(() -> {
      while (true) {
        try (@SuppressWarnings("unused") Socket clientSocket = socket.accept()) {
        } catch (IOException exception) {
          throw new UncheckedIOException(exception);
        }
      }
    }, "ListeningServer-" + socket.getLocalPort() + "-" + ID.getAndIncrement());
    t.start();
  }

  public int port() {
    return socket.getLocalPort();
  }

  void stop() throws IOException {
    socket.close();
  }

}
