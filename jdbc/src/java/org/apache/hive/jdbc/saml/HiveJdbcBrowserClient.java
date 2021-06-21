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
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.awt.Desktop;
import java.awt.Desktop.Action;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.service.auth.saml.HiveSamlUtils;
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
  private final ServerSocket serverSocket;
  private HiveJdbcBrowserServerResponse serverResponse;
  protected JdbcBrowserClientContext clientContext;
  // By default we wait for 2 min unless overridden by a JDBC connection param
  // browserResponseTimeout
  private static final int DEFAULT_SOCKET_TIMEOUT_SECS = 120;
  private final ExecutorService serverResponseThread = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("Hive-Jdbc-Browser-Client-%d")
          .setDaemon(true).build());

  HiveJdbcBrowserClient(JdbcConnectionParams connectionParams)
      throws HiveJdbcBrowserException {
    serverSocket = getServerSocket(connectionParams.getSessionVars());
  }

  private ServerSocket getServerSocket(Map<String, String> sessionConf)
      throws HiveJdbcBrowserException {
    final ServerSocket serverSocket;
    int port = Integer.parseInt(sessionConf
        .getOrDefault(JdbcConnectionParams.AUTH_BROWSER_RESPONSE_PORT, "0"));
    int timeout = Integer.parseInt(
        sessionConf.getOrDefault(JdbcConnectionParams.AUTH_BROWSER_RESPONSE_TIMEOUT_SECS,
            String.valueOf(DEFAULT_SOCKET_TIMEOUT_SECS)));
    try {
      serverSocket = new ServerSocket(port, 0,
          InetAddress.getByName(HiveSamlUtils.LOOP_BACK_INTERFACE));
      LOG.debug("Browser response timeout is set to {} seconds", timeout);
      serverSocket.setSoTimeout(timeout * 1000);
    } catch (IOException e) {
      throw new HiveJdbcBrowserException("Unable to bind to the localhost");
    }
    return serverSocket;
  }

  public Integer getPort() {
    return serverSocket.getLocalPort();
  }

  @Override
  public void close() throws IOException {
    if (serverSocket != null) {
      serverSocket.close();
    }
  }

  public void init(JdbcBrowserClientContext clientContext) {
    // everytime we set the sso URI we should clean up the previous state if its set.
    // this may be from the previous invalid connection attempt or if the token has
    // expired
    reset();
    this.clientContext = clientContext;
    LOG.trace("Initialized the JDBCBrowser client with URL {}",
        clientContext.getSsoUri());
  }

  private void reset() {
    serverResponse = null;
    clientContext = null;
  }

  public void doBrowserSSO() throws HiveJdbcBrowserException {
    Future<Void> serverResponseHandle = waitAsyncForServerResponse();
    logDebugInfoUri(clientContext.getSsoUri());
    openBrowserWindow();
    try {
      serverResponseHandle.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new HiveJdbcBrowserException(e);
    }
  }

  private void logDebugInfoUri(URI ssoURI) {
    Map<String, String> uriParams = new HashMap<>();
    try {
      uriParams = getQueryParams(ssoURI);
    } catch (HiveJdbcBrowserException e) {
      LOG.debug("Could get query params of the SSO URI", e);
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
    Preconditions.checkNotNull(ssoUri, "SSO Url is null");
    try {
      if (Desktop.isDesktopSupported() && Desktop.getDesktop()
          .isSupported(Action.BROWSE)) {
        Desktop.getDesktop().browse(ssoUri);
      } else {
        LOG.debug(
            "Desktop mode is not supported. Attempting to use OS "
                + "commands to open the default browser");
        //Desktop is not supported, lets try to open the browser process
        OsType os = getOperatingSystem();
        switch (os) {
          case WINDOWS:
            Runtime.getRuntime()
                .exec("rundll32 url.dll,FileProtocolHandler " + ssoUri.toString());
            break;
          case MAC:
            Runtime.getRuntime().exec("open " + ssoUri.toString());
            break;
          case LINUX:
            Runtime.getRuntime().exec("xdg-open " + ssoUri.toString());
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

  private Future<Void> waitAsyncForServerResponse() {
    return serverResponseThread.submit(() -> {
      // listen to the response on the server socket
      Socket socket;
      try {
        LOG.debug("Waiting for a server response on port {} with a timeout of {} ms",
            serverSocket.getLocalPort(), serverSocket.getSoTimeout());
        socket = serverSocket.accept();
      } catch (SocketTimeoutException timeoutException) {
        throw new HiveJdbcBrowserException(TIMEOUT_ERROR_MSG,
            timeoutException);
      } catch (IOException e) {
        throw new HiveJdbcBrowserException(
            "Unexpected error while listening on port " + serverSocket.getLocalPort()
                + " for server response", e);
      }
      try (BufferedReader reader = new BufferedReader(new InputStreamReader(
          socket.getInputStream(), StandardCharsets.UTF_8))) {
        char[] buffer = new char[16 * 1024];
        // block until you read into the buffer
        int len = reader.read(buffer);
        String response = String.valueOf(buffer, 0, len);
        String[] lines = response.split("\r\n");
        for (String line : lines) {
          if (!Strings.isNullOrEmpty(line)) {
            if (line.contains("token=")) {
              serverResponse = new HiveJdbcBrowserServerResponse(line);
              sendBrowserMsg(socket, serverResponse.isSuccessful());
            } else {
              LOG.trace("Skipping line {} from server response", line);
            }
          }
        }
        if (serverResponse == null) {
          throw new HiveJdbcBrowserException("Could not parse the response from server.");
        }
      } catch (IOException e) {
        throw new HiveJdbcBrowserException(
            "Unexpected exception while processing server response ", e);
      }
      return null;
    });
  }

  public HiveJdbcBrowserServerResponse getServerResponse() {
    return serverResponse;
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
              + "<body>Successfully authenticated. You may close this window.</body></html>";
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
