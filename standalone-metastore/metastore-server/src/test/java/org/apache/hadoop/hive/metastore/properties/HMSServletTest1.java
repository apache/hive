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
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.EntityEnclosingMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;

public class HMSServletTest1 extends HMSServletTest {

  @Override
  protected PropertyClient createClient(Configuration conf, int sport) throws Exception {
    URL url = new URL("http://hive@localhost:" + sport + "/" + CLI + "/" + NS);
    String jwt = generateJWT();
    return new JSonHttpClient(jwt, url.toString());
  }

  /**
   * A property client that uses Apache HttpClient as base.
   */
  public static class JSonHttpClient implements HttpPropertyClient {
    private final String uri;
    private final HttpClient client;
    private final String jwt;

    JSonHttpClient(String token, String uri) {
      this.jwt = token;
      this.uri = uri;
      this.client = new HttpClient();
    }

    private <M extends EntityEnclosingMethod> M prepareMethod(M method, String msgBody) throws IOException {
      method.addRequestHeader("Authorization", "Bearer " + jwt);
      method.addRequestHeader("Content-Type", "application/json");
      method.addRequestHeader("Accept", "application/json");
      method.addRequestHeader(MetaStoreUtils.USER_NAME_HTTP_HEADER, "hive");
      StringRequestEntity sre = new StringRequestEntity(msgBody, "application/json", "utf-8");
      method.setRequestEntity(sre);
      return method;
    }

    private boolean clientPut(Object args) throws IOException {
      PutMethod method = prepareMethod(new PutMethod(uri), new Gson().toJson(args));
      int httpStatus = client.executeMethod(method);
      if (HttpServletResponse.SC_OK == httpStatus) {
        return true;
      }
      return false;
    }

    private Object clientPost(Object args) throws IOException {
      PostMethod method = prepareMethod(new PostMethod(uri), new Gson().toJson(args));
      int httpStatus = client.executeMethod(method);
      if (HttpServletResponse.SC_OK == httpStatus) {
        String resp = method.getResponseBodyAsString();
        if (resp != null) {
          return new Gson().fromJson(resp, Object.class);
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
