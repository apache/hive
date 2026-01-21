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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.net.ssl.SSLContext;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class HMSHttpClientSslTest extends HMSJsonClientSslTest {
  @Override
  public void tearDown() throws Exception {
    if (client instanceof AutoCloseable) {
      ((AutoCloseable) client).close();
      client = null;
    }
    super.tearDown();
  }

  @Override
  protected PropertyClient createClient(Configuration conf, int sport) throws Exception {
    String path = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PATH);
    String scheme = getScheme(conf);
    URI uri = new URI(scheme + "://hive@localhost:" + sport + "/" + path + "/" + NS);
    String jwt = generateJWT();
    return new JsonHttpClient(conf, jwt, uri);
  }

  protected static HttpClient createHttpClient(Configuration conf) {
    SSLContext sslCtxt = clientSSLContextFactory(conf);
    return sslCtxt != null
            ? HttpClients.custom().setSSLContext(sslCtxt).setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).build()
            : HttpClients.createDefault();
  }

  /**
   * A property client that uses Apache HttpClient as base.
   */
  private static class JsonHttpClient implements HttpPropertyClient, AutoCloseable {
    private final URI uri;
    private final HttpClient client;
    private final String jwt;

    JsonHttpClient(Configuration conf, String token, URI uri) throws MalformedURLException {
      this.jwt = token;
      this.uri = uri;
      this.client = createHttpClient(conf);
    }
    @Override
    public void close() throws Exception {
      if (client instanceof AutoCloseable) {
        ((AutoCloseable) client).close();
      }
    }

    private <M extends HttpEntityEnclosingRequestBase> M prepareMethod(M method, String msgBody) throws IOException {
      method.addHeader("Authorization", "Bearer " + jwt);
      method.addHeader("Content-Type", "application/json");
      method.addHeader("Accept", "application/json");
      method.addHeader(MetaStoreUtils.USER_NAME_HTTP_HEADER, "hive");
      StringEntity sre = new StringEntity(msgBody, ContentType.APPLICATION_JSON);
      method.setEntity(sre);
      return method;
    }

    private boolean clientPut(Object args) throws IOException {
      HttpPut put = prepareMethod(new HttpPut(uri), new Gson().toJson(args));
      HttpResponse response = client.execute(put);
      try {
        return HttpServletResponse.SC_OK == response.getStatusLine().getStatusCode();
      } finally {
        if (response instanceof Closeable) {
          ((Closeable) response).close();
        }
      }
    }

    private Object clientPost(Object args) throws IOException {
      HttpPost post = prepareMethod(new HttpPost(uri), new Gson().toJson(args));
      HttpResponse response = client.execute(post);
      try {
        if (HttpServletResponse.SC_OK == response.getStatusLine().getStatusCode()) {
          HttpEntity entity = response.getEntity();
          if (entity != null) {
            Gson gson = new GsonBuilder().create();
            ContentType contentType = ContentType.getOrDefault(entity);
            Charset charset = contentType.getCharset();
            Reader reader = new InputStreamReader(entity.getContent(), charset);
            return gson.fromJson(reader,Object.class);
          }
        }
      } finally {
          if (response instanceof Closeable) {
            ((Closeable) response).close();
          }
      }
      return null;
    }

    @Override
    public boolean setProperties(Map<String, String> properties) {
      try {
        return clientPut(properties);
      } catch (IOException xio) {
        return false;
      }
    }

    @Override
    public Map<String, Map<String, String>> getProperties(String mapPrefix, String mapPredicate, String... selection) {
      Map<String, Object> args = new TreeMap<>();
      args.put("prefix", mapPrefix);
      if (mapPredicate != null) {
        args.put("predicate", mapPredicate);
      }
      if (selection != null && selection.length > 0) {
        args.put("selection", selection);
      }
      try {
        Object result = clientPost(args);
        return result instanceof Map ? (Map<String, Map<String, String>>) result : null;
      } catch (IOException xio) {
        return null;
      }
    }

    @Override
    public Map<String, String> getProperties(List<String> selection) throws IOException {
      try {
        Map<String, Object> args = new TreeMap<>();
        args.put("method", "fetchProperties");
        args.put("keys", selection);
        Object result = clientPost(args);
        return result instanceof Map ? (Map<String, String>) result : null;
      } catch (IOException xio) {
        return null;
      }
    }
  }

  @Test
  public void testPropertiesFilter() throws Exception {
    HttpClient httpClient = createHttpClient(conf);
    String jwt = generateJWT();
    NameValuePair[] nvp = new NameValuePair[]{
            new BasicNameValuePair("key", "db0.table01.fillFactor"),
            new BasicNameValuePair("key", "db0.table04.fillFactor")
    };
    String scheme = getScheme(conf);
    URI uri = new URIBuilder()
            .setScheme(scheme)
            .setUserInfo("hive")
            .setHost("localhost")
            .setPort(servletPort)
            .setPath("/" + path + "/" + NS)
            .setParameters(nvp)
            .build();
    HttpGet get = new HttpGet(uri);
    get.addHeader("Authorization", "Bearer " + jwt);
    get.addHeader("Content-Type", "application/json");
    get.addHeader("Accept", "application/json");
    get.addHeader(MetaStoreUtils.USER_NAME_HTTP_HEADER, "hive");

    Map<String,String> result = null;
    HttpResponse response = httpClient.execute(get);
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
      if (httpClient instanceof AutoCloseable) {
        ((AutoCloseable) httpClient).close();
      }
    }
  }


  @Test
  public void testEchoHttpClient() throws Exception {
    HttpClient client = createHttpClient(conf);
    String path = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PATH);
    String scheme = getScheme(conf);
    URI uri = new URI(scheme + "://hive@localhost:" + servletPort + "/" + path + "/" + NS);
    HttpResponse response = null;
    try {
      String jwt = generateJWT();
      String msgBody = "{\"method\":\"echo\"}";
      HttpPost post = new HttpPost(uri);
      post.addHeader("Authorization", "Bearer " + jwt);
      post.addHeader("Content-Type", "application/json");
      post.addHeader("Accept", "application/json");
      StringEntity sre = new StringEntity(msgBody, ContentType.APPLICATION_JSON);
      post.setEntity(sre);
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

}
