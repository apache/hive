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
package org.apache.hadoop.hive.metastore.properties;

import static org.eclipse.jetty.util.URIUtil.HTTP;
import static org.eclipse.jetty.util.URIUtil.HTTPS;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.PropertyServlet;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.eclipse.jetty.server.Server;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class HMSJsonClientSslTest extends HMSTestBase {
  String path = null;
  Server servletServer = null;
  int servletPort = -1;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    path = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PATH);
  }

  protected void setConf(Configuration conf) throws Exception {
    setHttpsConf(conf);
  }

  @Override
  protected int createServer(Configuration conf) throws Exception {
    if (servletServer == null) {
      setConf(conf);
      servletServer = PropertyServlet.startServer(conf);
      if (servletServer == null || !servletServer.isStarted()) {
        Assert.fail("http server did not start");
      }
      servletPort = servletServer.getURI().getPort();
    }
    return servletPort;
  }

  /**
   * Stops the server.
   * @param port the server port
   */
  @Override
  protected void stopServer(int port) throws Exception {
    if (servletServer != null) {
      servletServer.stop();
      servletServer = null;
      servletPort = -1;
    }
  }

  protected static String getScheme(Configuration conf) {
    return MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.USE_SSL) ? HTTPS : HTTP;
  }

  @Override
  protected PropertyClient createClient(Configuration conf, int port) throws Exception {
    String servletPath = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PATH);
    String scheme = getScheme(conf);
    URI uri = new URI(scheme + "://hive@localhost:" + port + "/" + servletPath + "/" + NS);
    String jwt = generateJWT();
    return new JsonClient(jwt, uri);
  }

  /**
   * A property client that uses http as transport.
   */
  @SuppressWarnings("unchecked")
  public class JsonClient implements HttpPropertyClient {
    private final URI uri;
    private final String jwt;
    JsonClient(String token, URI uri) {
      this.jwt = token;
      this.uri = uri;
    }

    public boolean setProperties(Map<String, String> properties) {
      try {
        clientCall(jwt, uri, "PUT", properties);
        return true;
      } catch(IOException xio) {
        return false;
      }
    }

    @Override
    public Map<String, Map<String, String>> getProperties(String mapPrefix, String mapPredicate, String... selection)  {
      Map<String, Object> args = new TreeMap<>();
      args.put("prefix", mapPrefix);
      if (mapPredicate != null) {
        args.put("predicate", mapPredicate);
      }
      if (selection != null && selection.length > 0) {
        args.put("selection", selection);
      }
      try {
        Object result = clientCall(jwt, uri, "POST", args);
        return result instanceof Map? (Map<String, Map<String, String>>) result : null ;
      } catch(IOException xio) {
        return null;
      }
    }

    @Override
    public Map<String, String> getProperties(List<String> selection) {
      try {
        Map<String, Object> args = new TreeMap<>();
        args.put("method", "fetchProperties");
        args.put("keys", selection);
        Object result = clientCall(jwt, uri, "POST", args);
        return result instanceof Map? (Map<String, String>) result : null ;
      } catch(IOException xio) {
        return null;
      }
    }
  }

  /**
   * Reads the content of a Reader into a String.
   * @param reader the reader to read from
   * @return the content as a String
   * @throws IOException if an I/O error occurs
   */
  protected static String readString(Reader reader) throws IOException {
    BufferedReader in = new BufferedReader(reader);
    String line;
    StringBuilder rslt = new StringBuilder();
    while ((line = in.readLine()) != null) {
      rslt.append(line);
    }
    return rslt.toString();
  }

  /**
   * Opens a connection to the given URL.
   * <p>This handles setting a socket factory suitable for https/ssl tests.</p>
   * @param url the URL to connect to
   * @return the HttpURLConnection
   * @throws IOException if an I/O error occurs
   */
  private HttpURLConnection openConnection(URL url) throws IOException {
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    if (con instanceof HttpsURLConnection httpsConnection) {
      SSLContext sslContext = clientSSLContextFactory(conf);
      if (sslContext != null) {
        httpsConnection.setSSLSocketFactory(sslContext.getSocketFactory());
        httpsConnection.setHostnameVerifier((hostname, session) -> true);
      }
    }
    return con;
  }

  /**
   * Performs a Json client call.
   * @param jwt the jwt token
   * @param uri the url
   * @param method the http method
   * @param arg the argument that will be transported as JSon
   * @return the result the was returned through Json
   * @throws IOException if marshalling the request/response fail
   */
  public Object clientCall(String jwt, URI uri, String method, Object arg) throws IOException {
    final HttpURLConnection con = openConnection(uri.toURL());
    con.setRequestMethod(method);
    con.setRequestProperty(MetaStoreUtils.USER_NAME_HTTP_HEADER, uri.getUserInfo());
    con.setRequestProperty("Content-Type", "application/json");
    con.setRequestProperty("Accept", "application/json");
    if (jwt != null) {
      con.setRequestProperty("Authorization","Bearer " + jwt);
    }
    con.setDoOutput(true);
    con.setDoInput(true);
    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
    wr.writeBytes(new Gson().toJson(arg));
    wr.flush();
    wr.close();
    int responseCode = con.getResponseCode();
    if (responseCode == HttpServletResponse.SC_OK) {
      try (Reader reader = new BufferedReader(
              new InputStreamReader(con.getInputStream(), StandardCharsets.UTF_8))) {
        return new Gson().fromJson(reader, Object.class);
      }
    }
    return null;
  }

  @Test
  public void testEcho() throws Exception {
    String path = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PATH);
    String scheme = getScheme(conf);
    URI uri = new URI(scheme + "://hive@localhost:" + servletPort + "/" + path + "/" + NS);
    Map<String, String> json = Collections.singletonMap("method", "echo");
    String jwt = generateJWT();
    // succeed
    Object response = clientCall(jwt, uri, "POST", json);
    Assert.assertNotNull(response);
    Assert.assertEquals(json, response);
    // fail (bad jwt)
    String badJwt = generateJWT(jwtUnauthorizedKeyFile.toPath());
    response = clientCall(badJwt, uri, "POST", json);
    Assert.assertNull(response);
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
