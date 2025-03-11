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
package org.apache.hadoop.hive.metastore;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.function.Function;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.eclipse.jetty.server.Server;
import org.junit.experimental.categories.Category;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hive.metastore.ServletServerBuilder.Descriptor;

@Category(MetastoreUnitTest.class)
public class TestServletServerBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(TestServletServerBuilder.class);

  private static Function<Configuration, Descriptor> describeServlet(final Map<String, Descriptor> descriptors, int port, String greeting) {
    return configuration -> {
      String name = greeting.toLowerCase();
      HttpServlet s1 = new HelloServlet(greeting) {
        @Override
        public String getServletName() {
          return name + "()";
        }
      };
      Descriptor descriptor = new Descriptor(port, name, s1);
      descriptors.put(s1.getServletName(), descriptor);
      return descriptor;
    };
  }

  @Test
  public void testOne() throws Exception {
    Configuration conf = new Configuration();
    // keeping track of what is built
    final Map<String, Descriptor> descriptors = new HashMap();
    Function<Configuration, Descriptor> fd1 = describeServlet(descriptors, 0, "ONE");
    Function<Configuration, Descriptor> fd2 = describeServlet(descriptors, 0, "TWO");
    // the 'conventional' way of starting the server
    Server server = ServletServerBuilder.startServer(LOG, conf, fd1, fd2);

    Descriptor d1 = descriptors.get("one()");
    Descriptor d2 = descriptors.get("two()");
    // same port for both servlets
    Assert.assertTrue(d1.getPort() > 0);
    Assert.assertEquals(d1.getPort(), d2.getPort());
    // check 
    URI uri = URI.create("http://localhost:" + d1.getPort());
    Object one = clientCall(uri.resolve("/one").toURL());
    Assert.assertEquals("ONE", one);
    uri = URI.create("http://localhost:" + d2.getPort());
    Object two = clientCall(uri.resolve("/two").toURL());
    Assert.assertEquals("TWO", two);
    server.stop();
  }

  @Test
  public void testOnePort() throws Exception {
    int port;
    try (ServerSocket server0 = new ServerSocket(0)) {
      port = server0.getLocalPort();
    } catch (IOException xio) {
      // cant run test if can not get free port
      return;
    }
    onePort(port);
  }

  @Test
  public void testOnePortAuto() throws Exception {
    onePort(0);
  }

  void onePort(int port) throws Exception {
    Configuration conf = new Configuration();
    ServletServerBuilder ssb = new ServletServerBuilder(conf);
    HttpServlet s1 = new HelloServlet("ONE");
    HttpServlet s2 = new HelloServlet("TWO");
    Descriptor d1 = ssb.addServlet(port, "one", s1);
    Descriptor d2 = ssb.addServlet(port, "two", s2);
    Server server = ssb.startServer();
    // same port for both servlets
    Assert.assertTrue(d1.getPort() > 0);
    Assert.assertEquals(d1.getPort(), d2.getPort());
    // check 
    URI uri = URI.create("http://localhost:" + d1.getPort());
    Object one = clientCall(uri.resolve("/one").toURL());
    Assert.assertEquals("ONE", one);
    uri = URI.create("http://localhost:" + d2.getPort());
    Object two = clientCall(uri.resolve("/two").toURL());
    Assert.assertEquals("TWO", two);
    server.stop();
  }

  @Test
  public void testTwoPorts() throws Exception {
    runTwoPorts(-1, -2);
  }

  @Test
  public void testTwoPortsAuto() throws Exception {
    int p0, p1;
    try (ServerSocket server0 = new ServerSocket(0); ServerSocket server1 = new ServerSocket(0)) {
      p0 = server0.getLocalPort();
      p1 = server1.getLocalPort();
    } catch (IOException xio) {
      // cant do test if can not get port
      return;
    }
    runTwoPorts(p0, p1);
  }

  void runTwoPorts(int p1, int p2) throws Exception {
    Configuration conf = new Configuration();
    ServletServerBuilder ssb = new ServletServerBuilder(conf);
    HttpServlet s1 = new HelloServlet("ONE");
    HttpServlet s2 = new HelloServlet("TWO");
    Descriptor d1 = ssb.addServlet(p1, "one", s1);
    Descriptor d2 = ssb.addServlet(p2, "two", s2);
    Map<Servlet, Integer> mappings = new IdentityHashMap<>();
    Server server = ssb.startServer();
    // different port for both servlets
    Assert.assertNotEquals(d1.getPort(), d2.getPort());

    URI uri = URI.create("http://localhost:" + d1.getPort());
    Object one = clientCall(uri.resolve("/one").toURL());
    Assert.assertEquals("ONE", one);
    // fail, not found
    Object o404 = clientCall(uri.resolve("/two").toURL());
    Assert.assertEquals(404, o404);
    uri = URI.create("http://localhost:" + d2.getPort());
    Object two = clientCall(uri.resolve("/two").toURL());
    Assert.assertEquals("TWO", two);
    // fail, not found
    o404 = clientCall(uri.resolve("/one").toURL());
    Assert.assertEquals(404, o404);
    server.stop();
  }

  static int findFreePort() throws IOException {
    try (ServerSocket server0 = new ServerSocket(0)) {
      return server0.getLocalPort();
    }
  }

  static int find2FreePort() throws IOException {
    try (ServerSocket socket0 = new ServerSocket(0)) {
      return socket0.getLocalPort();
    }
  }

  /**
   * Performs a Json client call.
   *
   * @param url the url
   * @return the result the was returned through Json
   * @throws IOException if marshalling the request/response fail
   */
  static Object clientCall(URL url) throws IOException {
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod("GET");
    con.setRequestProperty("Content-Type", "application/json");
    con.setRequestProperty("Accept", "application/json");
    con.setDoOutput(true);
    int responseCode = con.getResponseCode();
    if (responseCode == HttpServletResponse.SC_OK) {
      try (Reader reader = new BufferedReader(
              new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
        return new Gson().fromJson(reader, Object.class);
      }
    }
    return responseCode;
  }

}

class HelloServlet extends HttpServlet {

  final String greeting;

  public HelloServlet() {
    this("Hello");
  }

  public HelloServlet(String greeting) {
    this.greeting = greeting;
  }

  @Override
  protected void doGet(HttpServletRequest request,
          HttpServletResponse response) throws ServletException, IOException {
    response.setContentType("application/json");
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().println(greeting);
  }
}
