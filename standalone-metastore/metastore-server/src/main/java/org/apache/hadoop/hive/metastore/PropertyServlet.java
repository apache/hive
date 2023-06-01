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
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.Source;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The property-maps servlet.
 */
public class PropertyServlet extends HttpServlet {
  /** The common prefix for errors. */
  private static final String PTYERROR = "Property-maps servlet error ";
  /** The logger. */
  public static final Logger LOGGER = LoggerFactory.getLogger(PropertyServlet.class);
  /** The configuration. */
  private final Configuration configuration;
  /** The security. */
  private final ServletSecurity security;

  PropertyServlet(Configuration configuration) {
    String auth = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.PROPERTIES_SERVLET_AUTH);
    boolean jwt = auth != null && "jwt".equals(auth.toLowerCase());
    this.security = new ServletSecurity(configuration, jwt);
    this.configuration = configuration;
  }
  private String strError(String msg, Object...args) {
    return String.format(PTYERROR + msg, args);
  }

  private void sendError(HttpServletResponse response, Exception any, String msg) {
    int code = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
    if (any instanceof PropertyException || any instanceof NoSuchObjectException) {
      code = HttpServletResponse.SC_BAD_REQUEST;
    }
    sendError(response, code, msg);
  }
  private void sendError(HttpServletResponse response, int code, String msg) {
    try {
      response.sendError(code, msg);
    } catch(IOException ioeXception) {
      LOGGER.error(strError("sending error"), ioeXception);
      response.setStatus(code);
    }
  }

  private String getNamespace(String ruri) {
    int index = ruri.lastIndexOf("/");
    if (index > 1) {
      return ruri.substring(index + 1);
    }
    return "";
  }
  private RawStore getMS() throws ServletException {
      try {
        return HMSHandler.newRawStoreForConf(configuration);
      } catch(MetaException exception) {
        throw new ServletException(exception);
      }
  }

  private PropertyManager getPropertyManager(RawStore store, String ns) throws ServletException {
    try {
      PropertyStore propertyStore = store.getPropertyStore();
      return PropertyManager.create(ns, propertyStore);
    } catch (MetaException | NoSuchObjectException exception) {
      throw new ServletException(exception);
    }
  }

  private Object readJson(HttpServletRequest request) throws ServletException {
    try (Reader reader = new BufferedReader(
        new InputStreamReader(
            request.getInputStream(),
            StandardCharsets.UTF_8))) {
      return new Gson().fromJson(reader, Object.class);
    } catch (JsonIOException | JsonSyntaxException | IOException e) {
      throw new ServletException(e);
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
                       HttpServletResponse response) throws ServletException {
    final RawStore ms =  getMS();
    final String ns = getNamespace(request.getRequestURI());
    try {
      final PropertyManager mgr = getPropertyManager(ms, ns);
      // decode the request
      final Object json = readJson(request);
      // one or many actions imply...
      Iterable<?> actions = json instanceof List<?> ? (List<?>) json : Collections.singletonList(json);
      // ...one or many reactions
      List<Object> reactions = new ArrayList<>();
      String method = null;
      try {
        for (Object action : actions) {
          if (action instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked") Map<String, Object> call = (Map<String, Object>) action;
            method = (String) call.get("method");
            if (method == null) {
              method = "selectProperties";
            }
            switch (method) {
              // fetch a list of qualified keys by name
              case "fetchProperties": {
                // one or many keys
                Object jsonKeys = call.get("keys");
                if (jsonKeys == null) {
                  throw new IllegalArgumentException("null keys");
                }
                Iterable<?> keys = jsonKeys instanceof List<?>
                    ? (List<?>) jsonKeys
                    : Collections.singletonList(jsonKeys);
                Map<String, String> properties = new TreeMap<>();
                for (Object okey : keys) {
                  String key = okey.toString();
                  String value = mgr.exportPropertyValue(key);
                  if (value != null) {
                    properties.put(key, value);
                  }
                }
                reactions.add(properties);
                break;
              }
              // select a list of qualified keys by prefix/predicate/selection
              case "selectProperties": {
                String prefix = (String) call.get("prefix");
                if (prefix == null) {
                  throw new IllegalArgumentException("null prefix");
                }
                String predicate = (String) call.get("predicate");
                // selection may be null, a sole property or a list
                Object selection = call.get("selection");
                @SuppressWarnings("unchecked") List<String> project =
                    selection == null
                        ? null
                        : selection instanceof List<?>
                        ? (List<String>) selection
                        : Collections.singletonList(selection.toString());
                Map<String, PropertyMap> selected = mgr.selectProperties(prefix, predicate, project);
                Map<String, Map<String, String>> returned = new TreeMap<>();
                selected.forEach((k, v) -> returned.put(k, v.export(project == null)));
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
        writeJson(response, reactions.size() > 1 ? reactions : reactions.get(0));
        response.setStatus(HttpServletResponse.SC_OK);
      } catch (Exception any) {
        String error = strError("fetching values with %s, (%s) %s",
            method != null ? method : "?", any.getClass().getSimpleName(), any.getMessage());
        LOGGER.error(error, any);
        sendError(response, any, error);
        mgr.rollback();
      }
    } finally {
      ms.shutdown();
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
    security.execute(request, response, PropertyServlet.this::runPut);
  }
  private void runPut(HttpServletRequest request,
                       HttpServletResponse response) throws ServletException {
    final String ns = getNamespace(request.getRequestURI());
    final RawStore ms =  getMS();
    try {
      final PropertyManager mgr = getPropertyManager(ms, ns);
      Object json = readJson(request);
      if (json instanceof Map) {
        try {
          @SuppressWarnings("unchecked")
          Map<String, ?> cast = (Map<String, ?>) json;
          mgr.setProperties(cast);
          mgr.commit();
          response.setStatus(HttpServletResponse.SC_OK);
        } catch (Exception any) {
          String error = strError("setting values (%s) %s", any.getClass().getSimpleName(), any.getMessage());
          LOGGER.error(error, any);
          sendError(response, any, error);
          mgr.rollback();
        }
      } else {
        // no query was executed, no need to rollback
        String error = strError("setting values, bad argument type %s", json.getClass());
        LOGGER.error(error);
        sendError(response, HttpServletResponse.SC_BAD_REQUEST, error);
      }
    } finally {
      ms.shutdown();
    }
  }

  @Override
  protected void doGet(HttpServletRequest request,
                       HttpServletResponse response) throws ServletException, IOException {
    security.execute(request, response, PropertyServlet.this::runGet);
  }

  private void runGet(HttpServletRequest request,
                      HttpServletResponse response) throws ServletException {
    final String ns = getNamespace(request.getRequestURI());
    final RawStore ms = getMS();
    try {
      final PropertyManager mgr = getPropertyManager(ms, ns);
      try {
        String[] keys = request.getParameterValues("key");
        if (keys == null) {
            throw new IllegalArgumentException("null key");
        }
        Map<String, String> properties = new TreeMap<>();
        for (Object action : keys) {
          String key = action.toString();
          String value = mgr.exportPropertyValue(key);
          if (value != null) {
            properties.put(key, value);
          }
        }
        mgr.commit();
        // not an array if there was only one action
        writeJson(response, properties);
        response.setStatus(HttpServletResponse.SC_OK);
      } catch (Exception any) {
        mgr.rollback();
        String error = strError("getting values (%s) %s", any.getClass().getSimpleName(), any.getMessage());
        LOGGER.error(error, any);
        sendError(response, any, error);
      }
    } finally {
      ms.shutdown();
    }
  }

  /**
   * Convenience method to start a http server that only serves this servlet.
   * @param conf the configuration
   * @return the server instance
   * @throws Exception if servlet initialization fails
   */
  public static Server startServer(Configuration conf) throws Exception {
    // no port, no server
    int port = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PORT);
    if (port < 0) {
      return null;
    }
    String cli = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PATH);
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
    holder.setServlet(new PropertyServlet(conf)); //
    handler.addServletWithMapping(holder, "/"+cli+"/*");
    server.start();
    if (!server.isStarted()) {
      LOGGER.error("unable to start property-maps servlet server, path {}, port {}", cli, port);
    } else {
      LOGGER.info("started property-maps servlet server on {}", server.getURI());
    }
    return server;
  }
}
