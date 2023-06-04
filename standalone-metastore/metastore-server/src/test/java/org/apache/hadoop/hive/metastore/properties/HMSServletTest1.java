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
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

import javax.servlet.http.HttpServletResponse;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class HMSServletTest1 extends HMSServletTest {
  @Override
  public void tearDown() throws Exception {
    if (client instanceof AutoCloseable) {
      ((AutoCloseable) client).close();
    }
    super.tearDown();
  }

  @Override
  protected PropertyClient createClient(Configuration conf, int sport) throws Exception {
    URL url = new URL("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);
    String jwt = generateJWT();
    return new JSonHttpClient(jwt, url.toString());
  }

  /**
   * A property client that uses Apache HttpClient as base.
   */
  public static class JSonHttpClient implements HttpPropertyClient, AutoCloseable {
    private final String uri;
    private final HttpClient client;
    private final String jwt;

    JSonHttpClient(String token, String uri) {
      this.jwt = token;
      this.uri = uri;
      this.client = HttpClients.createDefault();
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

}
