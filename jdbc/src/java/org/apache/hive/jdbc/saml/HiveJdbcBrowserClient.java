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

package org.apache.hive.jdbc.saml;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.awt.Desktop;
import java.awt.Desktop.Action;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.service.auth.saml.HiveSamlUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to execute a browser based SSO workflow with the authentication mode
 * is browser.
 */
public class HiveJdbcBrowserClient implements IJdbcBrowserClient {

  private static final Logger LOG = LoggerFactory.getLogger(HiveJdbcBrowserClient.class);
  // error message when the socket times out.
  @VisibleForTesting
  public static final String TIMEOUT_ERROR_MSG = "Timed out while waiting for server response";
  // port as parsed from the connection URL; default is 0 which means any available port.
  private final int portFromUrl;
  // the actual port on the local machine where the web server is running
  private Integer serverPort;
  // timeout in mill-sec until which browserClient will wait for auth response from the
  // HS2 server.
  private final long timeoutInMs;
  private final BlockingQueue<HiveJdbcBrowserServerResponse>
      serverResponseQueue = new LinkedBlockingDeque<>();
  protected JdbcBrowserClientContext clientContext;
  // By default we wait for 2 min unless overridden by a JDBC connection param
  // browserResponseTimeout
  private static final int DEFAULT_SOCKET_TIMEOUT_SECS = 120;
  private Server webServer;
  private HiveJdbcBrowserServerResponse serverResponse;

  HiveJdbcBrowserClient(JdbcConnectionParams connectionParams)
      throws HiveJdbcBrowserException {
    portFromUrl = Integer.parseInt(connectionParams.getSessionVars()
        .getOrDefault(JdbcConnectionParams.AUTH_BROWSER_RESPONSE_PORT, "0"));
    timeoutInMs = Integer.parseInt(
        connectionParams.getSessionVars()
            .getOrDefault(JdbcConnectionParams.AUTH_BROWSER_RESPONSE_TIMEOUT_SECS,
                String.valueOf(DEFAULT_SOCKET_TIMEOUT_SECS))) * 1000L;
  }

  @Override
  public void startListening() throws HiveJdbcBrowserException {
    webServer = new Server();
    ServerConnector serverConnector = new ServerConnector(webServer);
    serverConnector.setHost(HiveSamlUtils.LOOP_BACK_INTERFACE);
    serverConnector.setPort(portFromUrl);
    LOG.info("Browser response timeout is set to {} ms", timeoutInMs);
    serverConnector.setIdleTimeout(timeoutInMs);
    webServer.addConnector(serverConnector);
    ServletHandler servletHandler = new ServletHandler();
    servletHandler.addServletWithMapping(
        new ServletHolder(new HttpBrowserClientServlet(this)), "/");
    webServer.setHandler(servletHandler);
    webServer.setStopTimeout(30*1000L);
    try {
      webServer.start();
      // we fetch the port after the server is started so that we can get the port
      // where the server has bound in case there is no port specified from the URL.
      serverPort = ((ServerConnector) webServer.getConnectors()[0]).getLocalPort();
      LOG.debug("Listening on the port {} ", serverPort);
    } catch (Exception e) {
      throw new HiveJdbcBrowserException("Could not start http server", e);
    }
  }

  public Integer getPort() {
    return serverPort;
  }

  @Override
  public String toString() {
    return "HiveJdbcBrowserClient@" + serverPort;
  }

  @Override
  public HiveJdbcBrowserServerResponse getServerResponse() {
    return serverResponse;
  }

  @Override
  public void close() throws IOException {
    if (webServer != null && webServer.isRunning()) {
      try {
        webServer.stop();
      } catch (Exception e) {
        throw new IOException(e);
      }
      webServer = null;
    }
  }

  public void init(JdbcBrowserClientContext clientContext) {
    // everytime we set the sso URI we should clean up the previous state if its set.
    // this may be from the previous invalid connection attempt or if the token has
    // expired
    reset();
    this.clientContext = clientContext;
    LOG.debug("Initialized the JDBCBrowser client with URL {}",
        clientContext.getSsoUri());
  }

  private void reset() {
    serverResponse = null;
    clientContext = null;
  }

  public void doBrowserSSO() throws HiveJdbcBrowserException {
    logDebugInfoUri(clientContext.getSsoUri());
    openBrowserWindow();
    try {
      waitForServerResponse(timeoutInMs);
    } catch (InterruptedException e) {
      throw new HiveJdbcBrowserException(e);
    }
    if (serverResponse == null) {
      throw new HiveJdbcBrowserException(TIMEOUT_ERROR_MSG);
    }
    if (!serverResponse.isValid()) {
      throw new HiveJdbcBrowserException(
          "Received invalid response from server. See driver logs for more details");
    }
  }

  private void logDebugInfoUri(URI ssoURI) {
    Map<String, String> uriParams = new HashMap<>();
    try {
      uriParams = getQueryParams(ssoURI);
    } catch (HiveJdbcBrowserException e) {
      LOG.info("Could get query params of the SSO URI", e);
    }
    LOG.debug("Initializing browser SSO request to URI. Relay state is {}",
        uriParams.get("RelayState"));
  }

  private Map<String, String> getQueryParams(URI ssoUri)
      throws HiveJdbcBrowserException {
    String decodedUrl;
    try {
      decodedUrl = URLDecoder
          .decode(ssoUri.toString(), StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new HiveJdbcBrowserException(e);
    }
    String[] params;
    try {
      params = new URI(decodedUrl).getQuery().split("&");
    } catch (URISyntaxException e) {
      throw new HiveJdbcBrowserException(e);
    }
    Map<String, String> paramMap = new HashMap<>();
    for (String param : params) {
      String key = param.split("=")[0];
      String val = param.split("=")[1];
      paramMap.put(key, val);
    }
    return paramMap;
  }

  @VisibleForTesting
  protected void openBrowserWindow() throws HiveJdbcBrowserException {
    URI ssoUri = clientContext.getSsoUri();
    try {
      if (Desktop.isDesktopSupported() && Desktop.getDesktop()
          .isSupported(Action.BROWSE)) {
        Desktop.getDesktop().browse(ssoUri);
      } else {
        LOG.info(
            "Desktop mode is not supported. Attempting to use OS "
                + "commands to open the default browser");
        String ssoUriStr = ssoUri.toString();
        //Desktop is not supported, lets try to open the browser process
        OsType os = getOperatingSystem();
        switch (os) {
          case WINDOWS:
            Runtime.getRuntime()
                .exec("rundll32 url.dll,FileProtocolHandler " + ssoUriStr);
            break;
          case MAC:
            Runtime.getRuntime().exec("open " + ssoUriStr);
            break;
          case LINUX:
            Runtime.getRuntime().exec("xdg-open " + ssoUriStr);
            break;
          case UNKNOWN:
            throw new HiveJdbcBrowserException(
                "Unknown operating system " + System.getProperty("os.name"));
        }
      }
    } catch (IOException e) {
      throw new HiveJdbcBrowserException("Unable to open browser to execute SSO", e);
    }
  }

  public void addServerResponse(HiveJdbcBrowserServerResponse response) {
    serverResponseQueue.add(response);
  }

  /**
   * Waits for a server response until the given timeout value in milliseconds.
   * Returns null if timeout.
   */
  private void waitForServerResponse(long timeoutInMs)
      throws InterruptedException {
     serverResponse = serverResponseQueue.poll(timeoutInMs, TimeUnit.MILLISECONDS);
  }

  public String getClientIdentifier() {
    if (clientContext == null) {
      return null;
    }
    return clientContext.getClientIdentifier();
  }

  private void sendBrowserMsg(Socket socket, boolean success) throws IOException {
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

    List<String> content = new ArrayList<>();
    content.add("HTTP/1.0 200 OK");
    content.add("Content-Type: text/html");
    String responseText;
    if (success) {
      responseText =
          "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>"
              + "<title>SAML Response Received</title></head>"
                  + "<body onload=\"waitAndClose()\">Successfully authenticated. You may close this window.</body>" +
                  "<script>" +
                  "  function waitAndClose() {" +
                  "    setTimeout(function() {" +
                  "      window.close()" +
                  "    }, 100);" +
                  "  }" +
                  "</script>"+"</html>";
    } else {
      responseText =
          "<!DOCTYPE html><html><head><meta charset=\"UTF-8\"/>"
              + "<title>SAML Response Received</title></head>"
              + "<body>Authentication failed. Please check server logs for details."
              + " You may close this window.</body></html>";
    }
    content.add(String.format("Content-Length: %s", responseText.length()));
    content.add("");
    content.add(responseText);

    for (int i = 0; i < content.size(); ++i) {
      if (i > 0) {
        out.print("\r\n");
      }
      out.print(content.get(i));
    }
    out.flush();
  }

  public OsType getMatchingOs(String osName) {
    osName = osName.toLowerCase();
    if (osName.contains("win")) {
      return OsType.WINDOWS;
    }
    if (osName.contains("mac")) {
      return OsType.MAC;
    }
    if (osName.contains("nix") || osName.contains("nux") || osName.contains("aix")) {
      return OsType.LINUX;
    }
    return OsType.UNKNOWN;
  }

  private enum OsType {
    WINDOWS,
    MAC,
    LINUX,
    UNKNOWN
  }

  private OsType getOperatingSystem() {
    String osName = System.getProperty("os.name");
    Preconditions.checkNotNull(osName, "os.name is null");
    return getMatchingOs(osName);
  }
}
