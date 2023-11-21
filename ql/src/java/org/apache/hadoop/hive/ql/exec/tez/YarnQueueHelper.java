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


package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.http.HttpStatus;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class YarnQueueHelper {
  private final static Logger LOG = LoggerFactory.getLogger(YarnQueueHelper.class);
  private static final String PERMISSION_PATH = "/ws/v1/cluster/queues/%s/access?user=%s";

  private final String[] rmNodes;
  private int lastKnownGoodUrl;
  private boolean sslForYarn;
  private boolean isHA;

  public YarnQueueHelper(HiveConf conf) {
    ArrayList<String> nodeList = new ArrayList<>();
    sslForYarn = YarnConfiguration.useHttps(conf);
    isHA = conf.getBoolean(YarnConfiguration.RM_HA_ENABLED, false);
    LOG.info("Yarn is using SSL: {}, HA enabled: {}", sslForYarn, isHA);

    if (isHA) {
      String[] rmids = conf.getStrings(YarnConfiguration.RM_HA_IDS);
      if (sslForYarn == true) {
        for (String rmid : rmids) {
          nodeList.addAll(Arrays.asList(conf
                  .getTrimmedStrings(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS + "." + rmid)));
        }
        Preconditions.checkArgument(nodeList.size() > 0,
            "yarn.resourcemanager.ha.rm-ids must be set to enable queue access checks in Yarn HA mode");
      }else{
        for (String rmid : rmids) {
          nodeList.addAll(Arrays.asList(conf.getTrimmedStrings(YarnConfiguration.RM_WEBAPP_ADDRESS + "."+rmid)));
          Preconditions.checkArgument(nodeList.size() > 0,
              "yarn.resourcemanager.ha.rm-ids must be set to enable queue access checks in Yarn HA mode");
        }
      }
      rmNodes = nodeList.toArray(new String[nodeList.size()]);
    }else {
      if (sslForYarn == true) {
        rmNodes = conf.getTrimmedStrings(YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS);
        Preconditions.checkArgument((rmNodes != null && rmNodes.length > 0),
            "yarn.resourcemanager.webapp.https.address must be set to enable queue access checks using TLS");
      } else {
        rmNodes = conf.getTrimmedStrings(YarnConfiguration.RM_WEBAPP_ADDRESS);
        Preconditions.checkArgument((rmNodes != null && rmNodes.length > 0),
            "yarn.resourcemanager.webapp.address must be set to enable queue access checks");
      }
    }
    lastKnownGoodUrl = 0;
  }

  /**
   * Checks yarn queue access of a given user for a given queue, and throws
   * HiveException in case the user doesn't have access to the queue.
   * @param  queueName the yarn queue name
   * @param  userName the username
   * @throws IOException when doAs throws an exception, we simply throw it
   * InterruptedException when doAs throws an exception, we simply throw it
   */
  public void checkQueueAccess(
      String queueName, String userName) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    try {
      ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
        checkQueueAccessInternal(queueName, userName);
        return null;
      });
    } catch (InterruptedException exception) {
      LOG.error("Cannot check queue access against UGI: " + ugi, exception);
      throw new IOException(exception);
    }
  }

  private void checkQueueAccessInternal(
      String queueName, String userName) throws IOException, HiveException {
    String urlSuffix = String.format(PERMISSION_PATH, queueName, userName);
    // TODO: if we ever use this endpoint for anything else, refactor cycling into a separate class.
    int urlIx = lastKnownGoodUrl, lastUrlIx = ((urlIx == 0) ? rmNodes.length : urlIx) - 1;
    Exception firstError = null;
    while (true) {
      String node = rmNodes[urlIx];
      String error = null;
      boolean isCallOk = false;
      String urlToCheck;
      if (sslForYarn){
        urlToCheck = "https://" + node + urlSuffix;
      }else{
        urlToCheck = "http://" + node + urlSuffix;
      }
      try {
        error = checkQueueAccessFromSingleRm(urlToCheck);
        isCallOk = true;
      } catch (Exception ex) {
        LOG.warn("Cannot check queue access against RM " + node, ex);
        if (firstError == null) {
          firstError = ex;
        }
      }
      if (isCallOk) {
        lastKnownGoodUrl = urlIx;
        if (error == null) return; // null error message here means the user has access.
        throw new HiveException(error.isEmpty()
            ? (userName + " has no access to " + queueName) : error);
      }
      if (urlIx == lastUrlIx) {
        throw new IOException("Cannot access any RM service; first error", firstError);
      }
      urlIx = (urlIx + 1) % rmNodes.length;
    }
  }

  private String checkQueueAccessFromSingleRm(String urlString) throws IOException {
    URL url = new URL(urlString);
    HttpURLConnection connection = UserGroupInformation.isSecurityEnabled() ?
        getSecureConnection(url) : (HttpURLConnection)url.openConnection();
    int statusCode = connection.getResponseCode();
    switch (statusCode) {
    case HttpStatus.SC_OK: return processResponse(connection);
    case HttpStatus.SC_FORBIDDEN: {
      // Throw a special exception since it's usually a well-known misconfiguration.
      throw new IOException(handleUnexpectedStatusCode(connection, statusCode, "check that the "
          + "HiveServer2 principal is in the administrator list of the root YARN queue"));
    }
    default: throw new IOException(handleUnexpectedStatusCode(connection, statusCode, null));
    }
  }

  private String processResponse(HttpURLConnection connection) throws IOException {
    InputStream stream = connection.getInputStream();
    if (stream == null) {
      throw new IOException(handleUnexpectedStatusCode(
          connection, HttpStatus.SC_OK, "No input on successful API call"));
    }
    String jsonStr = IOUtils.toString(stream);
    try {
      JSONObject obj = new JSONObject(jsonStr);
      boolean result = obj.getBoolean("allowed");
      if (result) return null;
      String diag = obj.getString("diagnostics");
      return diag == null ? "" : diag;
    } catch (JSONException ex) {
      LOG.error("Couldn't parse " + jsonStr, ex);
      throw ex;
    }

  }

  /** Gets the Hadoop kerberos secure connection (not an SSL connection). */
  private HttpURLConnection getSecureConnection(URL url) throws IOException {
    AuthenticatedURL.Token token = new AuthenticatedURL.Token();
    try {
      return new AuthenticatedURL().openConnection(url, token);
    } catch (AuthenticationException e) {
      throw new IOException(e);
    }
  }

  public String handleUnexpectedStatusCode(
      HttpURLConnection connection, int statusCode, String errorStr) throws IOException {
    // We do no handle anything but OK for now. Again, we need a real client for this API.
    // TODO: handle 401 and return a new connection? nothing for now
    InputStream errorStream = connection.getErrorStream();
    String error = "Received " + statusCode + (errorStr == null ? "" : (" (" + errorStr + ")"));
    if (errorStream != null) {
      error += ": " + IOUtils.toString(errorStream);
    } else {
      errorStream = connection.getInputStream();
      if (errorStream != null) {
        error += ": " + IOUtils.toString(errorStream);
      }
    }
    return error;
  }
}
