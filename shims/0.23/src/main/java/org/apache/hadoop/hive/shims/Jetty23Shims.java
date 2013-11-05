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

import java.io.IOException;

import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.handler.RequestLogHandler;
import org.mortbay.jetty.webapp.WebAppContext;

/**
 * Jetty23Shims.
 *
 */
public class Jetty23Shims implements JettyShims {
  public Server startServer(String listen, int port) throws IOException {
    Server s = new Server();
    s.setupListenerHostPort(listen, port);
    return s;
  }

  private static class Server extends org.mortbay.jetty.Server implements JettyShims.Server {
    public void addWar(String war, String contextPath) {
      WebAppContext wac = new WebAppContext();
      wac.setContextPath(contextPath);
      wac.setWar(war);
      RequestLogHandler rlh = new RequestLogHandler();
      rlh.setHandler(wac);
      this.addHandler(rlh);
    }

    public void setupListenerHostPort(String listen, int port)
        throws IOException {

      SocketConnector connector = new SocketConnector();
      connector.setPort(port);
      connector.setHost(listen);
      this.addConnector(connector);
    }
  }
}
