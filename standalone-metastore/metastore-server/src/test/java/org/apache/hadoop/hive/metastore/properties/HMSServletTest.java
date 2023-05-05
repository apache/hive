/* * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.hadoop.hive.metastore.properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.PropertyServlet;
import org.eclipse.jetty.server.Server;
import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class HMSServletTest extends HMSTestBase {
  // the url part
  private static final String CLI = "hmscli";
  Server servletServer = null;
  int sport = -1;

  @Override protected int createServer(Configuration conf) throws Exception {
    // need store before server for servlet
    if (objectStore == null) {
      boolean inited = createStore(conf);
      LOG.info("MetaStore store initialization " + (inited ? "successful" : "failed"));
    }
    if (servletServer == null) {
      servletServer = PropertyServlet.startServer(conf, CLI, objectStore);
      if (!servletServer.isStarted()) {
        Assert.fail("http server did not start");
      }
      sport = servletServer.getURI().getPort();
    }
    return sport;
  }

  /**
   * Stops the server.
   * @param port the server port
   */
  @Override protected void stopServer(int port) throws Exception {
    if (servletServer != null) {
      servletServer.stop();
      servletServer = null;
      sport = -1;
    }
  }

  @Override
  protected JSonClient createClient(Configuration conf, int sport) throws Exception {
    URL url = new URL("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);
    return new JSonClient(url);
  }

  public static class JSonClient implements PropertyClient {
    private final URL url;
    JSonClient(URL url) {
      this.url = url;
    }

    public boolean setProperties(Map<String, String> properties) {
      try {
        PropertyServlet.clientCall(url, "PUT", properties);
        return true;
      } catch(IOException xio) {
        return false;
      }
    }

    public Map<String, Map<String, String>> getProperties(String mapPrefix, String mapPredicate, String... selection) throws IOException {
      Map<String, Object> args = new TreeMap<>();
      args.put("prefix", mapPrefix);
      if (mapPredicate != null) {
        args.put("predicate", mapPredicate);
      }
      if (selection != null && selection.length > 0) {
        args.put("selection", selection);
      }
      try {
        Object result = PropertyServlet.clientCall(url, "POST", args);
        return result instanceof Map? (Map<String, Map<String, String>>) result : null ;
      } catch(IOException xio) {
        return null;
      }
    }
  }

  @Test
  public void testJSONServlet() throws Exception {
      URL url = new URL("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);
      Map<String, String> json = Collections.singletonMap("method", "echo");
      Object response = PropertyServlet.clientCall(url, "POST", json);
      Assert.assertNotNull(response);
      Assert.assertEquals(json, response);;
  }

  @Test
  public void testProperties1() throws Exception {
      runOtherProperties1(client);

  }

  @Test
  public void testProperties0() throws Exception {
      runOtherProperties0(client);
  }

}
