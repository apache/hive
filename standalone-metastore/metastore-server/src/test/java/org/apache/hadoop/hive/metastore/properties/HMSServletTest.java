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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.PropertyServlet;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.eclipse.jetty.server.Server;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class HMSServletTest extends HMSTestBase {
  protected static final String CLI = "hmscli";
  Server servletServer = null;
  int sport = -1;


  @Override protected int createServer(Configuration conf) throws Exception {
    if (servletServer == null) {
      servletServer = PropertyServlet.startServer(conf);
      if (servletServer == null || !servletServer.isStarted()) {
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
  protected PropertyClient createClient(Configuration conf, int sport) throws Exception {
    URL url = new URL("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);
    String jwt = generateJWT();
    return new JSonClient(jwt, url);
  }

  /**
   * A property client that uses http as transport.
   */
  @SuppressWarnings("unchecked")
  public static class JSonClient implements HttpPropertyClient {
    private final URL url;
    private final String jwt;
    JSonClient(String token, URL url) {
      this.jwt = token;
      this.url = url;
    }

    public boolean setProperties(Map<String, String> properties) {
      try {
        clientCall(jwt, url, "PUT", properties);
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
        Object result = clientCall(jwt, url, "POST", args);
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
        Object result = clientCall(jwt, url, "POST", args);
        return result instanceof Map? (Map<String, String>) result : null ;
      } catch(IOException xio) {
        return null;
      }
    }
  }

  @Test
  public void testServletEchoA() throws Exception {
    URL url = new URL("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);
    Map<String, String> json = Collections.singletonMap("method", "echo");
    String jwt = generateJWT();
    // succeed
    Object response = clientCall(jwt, url, "POST", json);
    Assert.assertNotNull(response);
    Assert.assertEquals(json, response);
    // fail (bad jwt)
    String badJwt = generateJWT(jwtUnauthorizedKeyFile.toPath());
    response = clientCall(badJwt, url, "POST", json);
    Assert.assertNull(response);
  }

  @Test
  public void testProperties1() throws Exception {
      runOtherProperties1(client);
  }

  @Test
  public void testProperties0() throws Exception {
      runOtherProperties0(client);

    HttpClient client = HttpClients.createDefault();
    String jwt = generateJWT();
    NameValuePair[] nvp = new NameValuePair[]{
        new BasicNameValuePair("key", "db0.table01.fillFactor"),
        new BasicNameValuePair("key", "db0.table04.fillFactor")
    };
    URI uri = new URIBuilder()
        .setScheme("http")
        .setUserInfo("hive")
        .setHost("localhost")
        .setPort(sport)
        .setPath("/" + CLI + "/" + NS)
        .setParameters(nvp)
        .build();
    HttpGet get = new HttpGet(uri);
    get.addHeader("Authorization", "Bearer " + jwt);
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    get.addHeader(MetaStoreUtils.USER_NAME_HTTP_HEADER, "hive");

    Map<String,String> result = null;
    HttpResponse response = client.execute(get);
    try {
      Assert.assertEquals(HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());
      HttpEntity entity = response.getEntity();
      if (entity != null) {
        Gson gson = new GsonBuilder().create();
        ContentType contentType = ContentType.getOrDefault(entity);
        Charset charset = contentType.getCharset();
        Reader reader = new InputStreamReader(entity.getContent(), charset);
        result = (Map<String, String>) gson.fromJson(reader, Object.class);
      }
      Assert.assertNotNull(result);
      Assert.assertEquals(2, result.size());
    } finally {
      if (response instanceof AutoCloseable) {
        ((AutoCloseable) response).close();
      }
      if (client instanceof AutoCloseable) {
        ((AutoCloseable) client).close();
      }
    }
  }

  private String readString(Reader reader) throws IOException {
    BufferedReader in = new BufferedReader(reader);
    String line = null;
    StringBuilder rslt = new StringBuilder();
    while ((line = in.readLine()) != null) {
      rslt.append(line);
    }
    return rslt.toString();
  }

  @Test
  public void testServletEchoB() throws Exception {
    HttpClient client = HttpClients.createDefault();
    HttpResponse response = null;
    try {
      String jwt = generateJWT();
      String msgBody = "{\"method\":\"echo\"}";
      HttpPost post = createPost(jwt, msgBody);

      response = client.execute(post);
      Assert.assertEquals(HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());
      String resp = null;
      HttpEntity entity = response.getEntity();
      if (entity != null) {
        ContentType contentType = ContentType.getOrDefault(entity);
        Charset charset = contentType.getCharset();
        Reader reader = new InputStreamReader(entity.getContent(), charset);
        resp = readString(reader);
      }
      Assert.assertNotNull(resp);
      Assert.assertEquals(msgBody, resp);
    } finally {
      if (response instanceof AutoCloseable) {
        ((AutoCloseable) response).close();
      }
      if (client instanceof AutoCloseable) {
        ((AutoCloseable) client).close();
      }
    }
  }

  /**
   * Performs a Json client call.
   * @param jwt the jwt token
   * @param url the url
   * @param method the http method
   * @param arg the argument that will be transported as JSon
   * @return the result the was returned through Json
   * @throws IOException if marshalling the request/response fail
   */
  public static Object clientCall(String jwt, URL url, String method, Object arg) throws IOException {
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod(method);
    con.setRequestProperty(MetaStoreUtils.USER_NAME_HTTP_HEADER, url.getUserInfo());
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

  /**
   * Create a PostMethod populated with the expected attributes.
   * @param jwt the security token
   * @param msgBody the actual (json) payload
   * @return the method to be executed by a Http client
   * @throws Exception
   */
  private HttpPost createPost(String jwt, String msgBody) {
    HttpPost method = new HttpPost("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);
    method.addHeader("Authorization", "Bearer " + jwt);
    method.addHeader("Content-Type", "application/json");
    method.addHeader("Accept", "application/json");

    StringEntity sre = new StringEntity(msgBody, ContentType.APPLICATION_JSON);
    method.setEntity(sre);
    return method;
  }

}
