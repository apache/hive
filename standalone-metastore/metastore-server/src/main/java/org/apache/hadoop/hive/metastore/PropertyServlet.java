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

package org.apache.hadoop.hive.metastore;

import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.properties.PropertyException;
import org.apache.hadoop.hive.metastore.properties.PropertyManager;
import org.apache.hadoop.hive.metastore.properties.PropertyMap;
import org.apache.hadoop.hive.metastore.properties.PropertyStore;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.Source;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The property  cli servlet.
 */
public class PropertyServlet extends HttpServlet {
  /** The logger. */
  public static final Logger LOGGER = LoggerFactory.getLogger(PropertyServlet.class);
  /** The object store. */
  private final RawStore objectStore;
  /** The security. */
  private final ServletSecurity security;

  PropertyServlet(Configuration configuration, RawStore store) {
    this.security = new ServletSecurity(configuration, false);
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
    return PropertyManager.create(ns, propertyStore);
  }

  private Object readJson(HttpServletRequest request) throws ServletException, IOException {
    ServletInputStream inputStream = request.getInputStream();
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

  public void init() throws ServletException {
    super.init();
    security.init();
  }

  @Override
  protected void doPost(HttpServletRequest request,
                        HttpServletResponse response) throws ServletException, IOException {
    security.execute(request, response, PropertyServlet.this::runPost);
  }

  private void runPost(HttpServletRequest request,
                       HttpServletResponse response) throws ServletException, IOException {
    String ns = getNamespace(request.getRequestURI());
    PropertyManager mgr;
    try {
      mgr = getPropertyManager(ns);
    } catch (MetaException | NoSuchObjectException exception) {
      throw new ServletException(exception);
    }
    Object json = readJson(request);
    // one or many actions imply...
    Iterable<?> actions = json instanceof List<?> ? (List<?>) json : Collections.singletonList(json);
    // ...one or many reactions
    List<Object> reactions = new ArrayList<>();
    try {
      for (Object action : actions) {
        if (action instanceof Map<?,?>) {
          @SuppressWarnings("unchecked")  Map<String, Object> call = (Map<String, Object>) action;
          String method = (String) call.get("method");
          if (method == null) {
            method = "selectProperties";
          }
          switch (method) {
            case "selectProperties": {
              String prefix = (String) call.get("prefix");
              if (prefix == null) {
                throw new IllegalArgumentException("null prefix");
              }
              String predicate = (String) call.get("predicate");
              @SuppressWarnings("unchecked") List<String> project = (List<String>) call.get("selection");
              Map<String, PropertyMap> selected = mgr.selectProperties(prefix, predicate, project);
              Map<String, Map<String, String>> returned = new TreeMap<>();
              selected.forEach((k, v) -> returned.put(k, v.export()));
              reactions.add(returned);
              break;
            }
            case "script": {
              String src = (String) call.get("source");
              reactions.add(mgr.runScript(src));
              break;
            }
            case "echo": {
              reactions.add(action);
              break;
            }
            default: {
              throw new IllegalArgumentException("bad argument type " + action.getClass());
            }
          }
        }
      }
      mgr.commit();
      // not an array if there was only one action
      writeJson(response, reactions.size() > 1? reactions : reactions.get(0));
      response.setStatus(HttpServletResponse.SC_OK);
    } catch (PropertyException any) {
      mgr.rollback();
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "servlet fail, property error " + any);
    } catch (Exception any) {
      mgr.rollback();
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "servlet fail " + any);
    }
  }

//  A way to import values using files sent over http
//  private void importProperties(HttpServletRequest request) throws ServletException, IOException {
//    List<Part> fileParts = request.getParts().stream()
//        .filter(part -> "files".equals(part.getName()) && part.getSize() > 0)
//        .collect(Collectors.toList()); // Retrieves <input type="file" name="files" multiple="true">
//
//    for (Part filePart : fileParts) {
//      String fileName = Paths.get(filePart.getSubmittedFileName()).getFileName().toString(); // MSIE fix.
//      InputStream fileContent = filePart.getInputStream();
//      // ... (do your job here)
//    }
//  }

  @Override
  protected void doPut(HttpServletRequest request,
                        HttpServletResponse response) throws ServletException, IOException {
    security.execute(request, response, PropertyServlet.this::runGet);
  }
  private void runGet(HttpServletRequest request,
                       HttpServletResponse response) throws ServletException, IOException {

    String ns = getNamespace(request.getRequestURI());
    PropertyManager mgr;
    try {
      mgr = getPropertyManager(ns);
    } catch (MetaException | NoSuchObjectException exception) {
      throw new ServletException(exception);
    }
    Object json = readJson(request);
    if (json instanceof Map) {
      try {
        @SuppressWarnings("unchecked")
        Map<String, ?> cast = (Map<String, ?>) json;
        mgr.setProperties(cast);
        mgr.commit();
        response.setStatus(HttpServletResponse.SC_OK);
        return;
      } catch (Exception any) {
        mgr.rollback();
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "select fail " + any);
      }
    }
    response.sendError(HttpServletResponse.SC_BAD_REQUEST, "bad argument type " + json.getClass());
  }

  @Override
  protected void doGet(HttpServletRequest request,
                       HttpServletResponse response) throws ServletException, IOException {
    security.execute(request, response, PropertyServlet.this::runGet);
  }

  /**
   * Performs a Json client call.
   * @param url the url
   * @param method the http method
   * @param arg the argument that will be transported as JSon
   * @return the result the was returned through Json
   * @throws IOException if marshalling the request/response fail
   */
   public static Object clientCall(URL url, String method, Object arg) throws IOException {
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    con.setRequestMethod(method);
    con.setRequestProperty(ServletSecurity.X_USER, url.getUserInfo());
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

  /**
   * Convenience method to start a http server that only serves this servlet.
   * @param conf the configuration
   * @param cli the url part
   * @param store the store
   * @return the server instance
   * @throws Exception if servlet initialization fails
   */
  public static Server startServer(Configuration conf, String cli, RawStore store) throws Exception {
    // no port, no server
    int port = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PORT);
    if (port < 0) {
      return null;
    }
    // HTTP Server
    Server server = new Server();
    server.setStopAtShutdown(true);

    // Optional SSL
    final SslContextFactory sslContextFactory = ServletSecurity.createSslContextFactory(conf);
    final ServerConnector connector = new ServerConnector(server, sslContextFactory);
    connector.setPort(port);
    connector.setReuseAddress(true);
    server.addConnector(connector);

    // Hook the servlet
    ServletHandler handler = new ServletHandler();
    server.setHandler(handler);
    ServletHolder holder = handler.newServletHolder(Source.EMBEDDED);
    holder.setServlet(new PropertyServlet(conf, store)); //
    handler.addServletWithMapping(holder, "/"+cli+"/*");
    server.start();
    return server;
  }
}
