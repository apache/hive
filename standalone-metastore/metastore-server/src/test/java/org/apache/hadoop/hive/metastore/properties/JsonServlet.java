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
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.Source;
import org.junit.Assert;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A skeleton servlet for tests.
 */
public class JsonServlet extends HttpServlet {
  /** The object store. */
  protected RawStore objectStore = null;

  JsonServlet(RawStore store) {
    this.objectStore = store;
  }

  private String getNamespace(String ruri) {
    int index = ruri.lastIndexOf("/");
    if (index > 1) {
      return ruri.substring(index + 1);
    }
    return "";
  }

  private PropertyManager getPropertyManager(String ns) throws MetaException, NoSuchObjectException {
    PropertyStore propertyStore = objectStore.getPropertyStore();
    PropertyManager mgr = PropertyManager.create(ns, propertyStore);
    return mgr;
  }

  private Object readJson(HttpServletRequest request) throws ServletException, IOException {
    ServletInputStream inputStream = request.getInputStream();
    Object json = null;
    try (Reader reader = new BufferedReader(
        new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      return new Gson().fromJson(reader, Object.class);
    } catch (JsonIOException | JsonSyntaxException | IOException e) {
      throw new ServletException("post failed for HmsJSONHttpServlet."
          + " Error: " + e);
    }
  }

  private void writeJson(HttpServletResponse response, Object value) throws IOException {
    ServletOutputStream outputStream = response.getOutputStream();
    response.setStatus(HttpServletResponse.SC_OK);
    PrintWriter writer = new PrintWriter(outputStream);
    writer.write(new Gson().toJson(value));
    writer.flush();
  }

  @Override
  protected void doPost(HttpServletRequest request,
                        HttpServletResponse response) throws ServletException, IOException {
    String ns = getNamespace(request.getRequestURI());
    PropertyManager mgr;
    try {
      mgr = getPropertyManager(ns);
    } catch (MetaException | NoSuchObjectException xpty) {
      throw new ServletException(xpty);
    }
    Object json = readJson(request);
    if (json instanceof Map) {
      Map<String, Object> call = (Map<String, Object>) json;
      String method = (String) call.get("method");
      if (method == null || "selectProperties".equals(method)) {
        String prefix = (String) call.get("prefix");
        if (prefix == null) {
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "null prefix");
        }
        String predicate = (String) call.get("predicate");
        List<String> project = (List<String>) call.get("selection");
        try {
          Map<String, PropertyMap> selected = mgr.selectProperties(prefix, predicate, project);
          Map<String, Map<String, String>> returned = new TreeMap<>();
          selected.forEach((k, v) -> {
            returned.put(k, v.export());
          });
          mgr.commit();
          writeJson(response, returned);
          response.setStatus(HttpServletResponse.SC_OK);
        } catch (Exception xany) {
          mgr.rollback();
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "select fail " + xany);
        }
        return;
      } else if ("echo".equals(method)) {
        writeJson(response, json);
        response.setStatus(HttpServletResponse.SC_OK);
        return;
      }
    }
    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "bad argument type " + json.getClass());
  }

  @Override
  protected void doPut(HttpServletRequest request,
                       HttpServletResponse response) throws ServletException, IOException {
    String ns = getNamespace(request.getRequestURI());
    PropertyManager mgr;
    try {
      mgr = getPropertyManager(ns);
    } catch (MetaException | NoSuchObjectException xpty) {
      throw new ServletException(xpty);
    }
    Object json = readJson(request);
    if (json instanceof Map) {
      try {
        mgr.setProperties((Map) json);
        mgr.commit();
        response.setStatus(HttpServletResponse.SC_OK);
        return;
      } catch (Exception xany) {
        mgr.rollback();
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "select fail " + xany);
      }
    }
    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "bad argument type " + json.getClass());
  }

  @Override
  protected void doGet(HttpServletRequest request,
                       HttpServletResponse response) throws ServletException, IOException {
    String ns = getNamespace(request.getRequestURI());
    Object json = readJson(request);
    writeJson(response, json);
  }

  /**
   * Performs a Json client call.
   * @param url the url
   * @param method the http method
   * @param arg the argument that will be transported as JSon
   * @return the result the was returned through Json
   * @throws IOException
   */
   static Object clientCall(URL url, String method, Object arg) throws IOException {
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod(method);
    con.setRequestProperty("Content-Type", "application/json");
    con.setRequestProperty("Accept", "application/json");
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
  static Server startServer(Configuration conf, String cli, RawStore store) throws Exception {
    Server server = new Server(0);
    ServletHandler handler = new ServletHandler();
    server.setHandler(handler);
    //ServletContextHandler context = new ServletContextHandler(
    //    ServletContextHandler.NO_SECURITY | ServletContextHandler.NO_SESSIONS);
    // context.addServlet(new ServletHolder(JsonServlet.class),  "/testJSONServlet");
    ServletHolder holder = handler.newServletHolder(Source.EMBEDDED);
    holder.setServlet(new JsonServlet(store)); //
    handler.addServletWithMapping(holder, "/"+cli+"/*");
    server.start();
    return server;
  }
}
