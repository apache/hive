/**
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
package org.apache.hadoop.hive.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test CliSessionState
 */
public class TestCliSessionState {

  private static TCPServer server;
  private static String command = null;

  @BeforeClass
  public static void start() throws Exception {
    // start fake server
    server = new TCPServer();
    Thread thread = new Thread(server);
    thread.start();
    // wait for start server;
    while (server.getPort() == 0) {
      Thread.sleep(20);
    }
  }

  @AfterClass
  public static void stop() throws IOException {
    server.stop();
  }

  /**
   * test CliSessionState for remote
   */
  @Test
  public void testConnect() throws Exception {
    CliSessionState sessionState = new CliSessionState(new HiveConf());
    sessionState.port = server.getPort();
    sessionState.setHost(InetAddress.getLocalHost().getHostName());
    // check connect
    sessionState.connect();
    assertTrue(sessionState.isRemoteMode());
    assertEquals(server.getPort(), sessionState.getPort());
    assertEquals(InetAddress.getLocalHost().getHostName(), sessionState.getHost());
    assertNotNull(sessionState.getClient());
    sessionState.close();
    // close should send command clean
    assertEquals(command, "clean");

  }

  /**
   * test default db name
   */
  @Test
  public void testgetDbName() throws Exception {
    SessionState.start(new HiveConf());
    assertEquals(MetaStoreUtils.DEFAULT_DATABASE_NAME,
        SessionState.get().getCurrentDatabase());
  }

  /**
   * fake hive server
   */
  private static class TCPServer implements Runnable {
    private int port = 0;
    private boolean stop = false;
    private ServerSocket welcomeSocket;

    public void run() {
      try {

        welcomeSocket = new ServerSocket(0);
        port = welcomeSocket.getLocalPort();
        while (!stop) {
          byte[] buffer = new byte[512];
          Socket connectionSocket = welcomeSocket.accept();
          InputStream input = connectionSocket.getInputStream();
          OutputStream output = connectionSocket.getOutputStream();
          int read = input.read(buffer);
          // command without service bytes
          command = new String(buffer, 8, read - 13);
          // send derived
          output.write(buffer, 0, read);
        }
      } catch (IOException e) {
        ;
      }

    }

    public int getPort() {
      return port;
    }

    public void stop() throws IOException {
      stop = true;
      welcomeSocket.close();
    }
  }
}
