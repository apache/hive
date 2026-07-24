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
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.properties.PropertyException;
import org.apache.hadoop.hive.metastore.properties.PropertyManager;
import org.apache.hadoop.hive.metastore.properties.PropertyMap;
import org.apache.hadoop.hive.metastore.properties.PropertyStore;
import org.eclipse.jetty.server.Server;
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
 * HTTP servlet exposing the {@link org.apache.hadoop.hive.metastore.properties.PropertyManager}
 * API over JSON.
 *
 * <p>The servlet is mounted at a configurable path and port (see
 * {@code MetastoreConf.ConfVars.PROPERTIES_SERVLET_PORT} and
 * {@code MetastoreConf.ConfVars.PROPERTIES_SERVLET_PATH}). The last segment of the request URI is
 * used as the property manager <em>namespace</em>, so the same servlet instance can serve multiple
 * independent namespaces.</p>
 *
 * <h2>Namespace and Schema resolution</h2>
 * <p>The last URI segment is the <em>namespace</em>. Each namespace must be registered in advance
 * by calling {@link org.apache.hadoop.hive.metastore.properties.PropertyManager#declare(String, Class)}
 * with a concrete {@link org.apache.hadoop.hive.metastore.properties.PropertyManager} subclass.
 * That subclass is instantiated per request via its {@code (String, PropertyStore)} constructor.</p>
 *
 * <p>Within a namespace, the {@link org.apache.hadoop.hive.metastore.properties.PropertySchema} is
 * <em>not</em> fixed — it is resolved dynamically by the manager from the number of dot-separated
 * key fragments. For example, {@link org.apache.hadoop.hive.metastore.properties.HMSPropertyManager}
 * (registered under {@code "hms"}) uses:</p>
 * <table border="1">
 *   <caption>HMSPropertyManager key-to-schema mapping</caption>
 *   <tr><th>Fragments</th><th>Example key</th><th>Schema</th></tr>
 *   <tr><td>1</td><td>{@code name}</td><td>CLUSTER_SCHEMA</td></tr>
 *   <tr><td>2</td><td>{@code db.name}</td><td>DATABASE_SCHEMA</td></tr>
 *   <tr><td>3+</td><td>{@code db.table.name}</td><td>TABLE_SCHEMA</td></tr>
 * </table>
 * <p>A namespace with no registered manager throws {@code NoSuchObjectException} (HTTP 400).</p>
 *
 * <h2>HTTP Methods</h2>
 *
 * <h3>GET — fetch properties by key</h3>
 * <p>One or more {@code key} query parameters select individual property values.
 * Returns a JSON object mapping each found key to its string value.</p>
 * <pre>{@code
 * GET /properties/mynamespace?key=db1.table1.owner&key=db1.table1.created_time
 * → { "db1.table1.owner": "alice", "db1.table1.created_time": "2024-01-15T08:00:00.00Z" }
 * }</pre>
 *
 * <h3>PUT — set properties</h3>
 * <p>Request body must be a JSON object mapping qualified keys to values. All updates are
 * committed atomically; any error triggers a rollback.</p>
 * <pre>{@code
 * PUT /properties/mynamespace
 * { "db1.table1.owner": "bob", "db1.table1.is_external": "true" }
 * }</pre>
 *
 * <h3>POST — query or script</h3>
 * <p>The request body is a single JSON action object or a JSON array of action objects.
 * Each action must have a {@code "method"} field (defaults to {@code "selectProperties"} if absent).
 * Returns a single result object, or an array when multiple actions are submitted.
 * All actions in one request share a transaction; any failure rolls back the entire batch.</p>
 *
 * <p>Supported methods:</p>
 * <dl>
 *   <dt>{@code fetchProperties}</dt>
 *   <dd>Fetches exact values for a list of fully-qualified keys.
 *   <pre>{@code
 * { "method": "fetchProperties", "keys": ["db1.table1.owner", "db1.table1.tags"] }
 * → { "db1.table1.owner": "alice" }
 *   }</pre></dd>
 *
 *   <dt>{@code selectProperties}</dt>
 *   <dd>Selects property maps whose key starts with {@code prefix}, optionally filtered by a
 *   JEXL {@code predicate} expression and projected to a subset of property names via
 *   {@code selection}.
 *   <pre>{@code
 * { "method": "selectProperties",
 *   "prefix": "db1.",
 *   "predicate": "owner == 'alice'",
 *   "selection": ["owner", "is_external"] }
 * → { "db1.table1": { "owner": "alice", "is_external": "true" } }
 *   }</pre></dd>
 *
 *   <dt>{@code script}</dt>
 *   <dd>Executes a JEXL script via {@link org.apache.hadoop.hive.metastore.properties.PropertyManager#runScript}.
 *   <pre>{@code
 * { "method": "script", "source": "select('db1.', null, null)" }
 *   }</pre></dd>
 *
 *   <dt>{@code echo}</dt>
 *   <dd>Returns the action object unchanged. Useful for health checks and round-trip testing.</dd>
 * </dl>
 *
 * <h2>Error handling</h2>
 * <p>{@link org.apache.hadoop.hive.metastore.properties.PropertyException} and
 * {@code NoSuchObjectException} map to HTTP 400 (Bad Request);
 * all other exceptions map to HTTP 500 (Internal Server Error).</p>
 *
 * <h2>Configuration</h2>
 * <ul>
 *   <li>{@code MetastoreConf.ConfVars.PROPERTIES_SERVLET_PORT} — listening port (negative disables the servlet)</li>
 *   <li>{@code MetastoreConf.ConfVars.PROPERTIES_SERVLET_PATH} — URL path prefix</li>
 *   <li>{@code MetastoreConf.ConfVars.PROPERTIES_SERVLET_AUTH} — authentication type</li>
 * </ul>
 */
public class PropertyServlet extends HttpServlet {
  /** The common prefix for errors. */
  private static final String PTYERROR = "Property-maps servlet error ";
  /** The logger. */
  public static final Logger LOGGER = LoggerFactory.getLogger(PropertyServlet.class);
  /** The configuration. */
  private final Configuration configuration;

  PropertyServlet(Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public String getServletName() {
    return "HMS Property";
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

  @Override
  public void init() throws ServletException {
    super.init();
  }

  @Override
  protected void doPost(HttpServletRequest request,
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
                fetchProperties( mgr, call, reactions);
                break;
              }
              // select a list of qualified keys by prefix/predicate/selection
              case "selectProperties": {
                selectProperties(mgr, call, reactions);
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
                throw new IllegalArgumentException("Bad argument type " + action.getClass());
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

  private static void fetchProperties(PropertyManager mgr, Map<String, Object> call, List<Object> reactions) {
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
  }

  private static void selectProperties(PropertyManager mgr, Map<String, Object> call, List<Object> reactions) {
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
  }

  @Override
  protected void doPut(HttpServletRequest request,
                       HttpServletResponse response) throws ServletException, IOException {
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

  public static ServletServerBuilder.Descriptor createServlet(Configuration configuration) {
    try {
      int port = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PORT);
      String path = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PATH);
      if (port >= 0 && path != null && !path.isEmpty()) {
        String authType = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.PROPERTIES_SERVLET_AUTH);
        ServletSecurity security = new ServletSecurity(AuthType.fromString(authType), configuration);
        HttpServlet servlet = security.proxy(new PropertyServlet(configuration));
        return new ServletServerBuilder.Descriptor(port, path, servlet);
      }
    } catch (Exception io) {
      LOGGER.error("Failed to create servlet ", io);
    }
    return null;
  }

  /**
   * Convenience method to start a http server that only serves this servlet.
   *
   * @param conf the configuration
   * @return the server instance
   * @throws Exception if servlet initialization fails
   */
  public static Server startServer(Configuration conf) throws Exception {
   return ServletServerBuilder.startServer(LOGGER, conf, PropertyServlet::createServlet);
  }

}
