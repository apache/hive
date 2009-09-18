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
package org.apache.hadoop.hive.shims;

import org.mortbay.http.SocketListener;
import org.mortbay.jetty.servlet.WebApplicationContext;

import java.io.IOException;

public class Jetty17Shims implements JettyShims {
  public Server startServer(String listen, int port) throws IOException {
    Server s = new Server();
    s.setupListenerHostPort(listen, port);
    return s;
  }

  private static class Server extends org.mortbay.jetty.Server implements JettyShims.Server {
    public void addWar(String war, String contextPath) {
      WebApplicationContext wac = new WebApplicationContext();
      wac.setContextPath(contextPath);
      wac.setWAR(war);
      addContext(wac);
    }

    public void setupListenerHostPort(String listen, int port)
      throws IOException {

      SocketListener listener = new SocketListener();
      listener.setPort(port);
      listener.setHost(listen);
      this.addListener(listener);
    }
  }
}