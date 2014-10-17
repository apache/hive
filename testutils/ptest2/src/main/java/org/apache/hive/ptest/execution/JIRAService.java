/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.hive.ptest.api.server.TestLogger;
import org.apache.hive.ptest.execution.conf.Context;
import org.apache.hive.ptest.execution.conf.TestConfiguration;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScheme;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthState;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

class JIRAService {
  static final int MAX_MESSAGES = 200;
  static final String TRIMMED_MESSAGE = "**** This message was trimmed, see log for full details ****";
  private final Logger mLogger;
  private final String mName;
  private final String mBuildTag;
  private final String mPatch;
  private final String mUrl;
  private final String mUser;
  private final String mPassword;
  private final String mJenkinsURL;
  private final String mLogsURL;

  public JIRAService(Logger logger, TestConfiguration configuration, String buildTag) {
    mLogger = logger;
    mName = configuration.getJiraName();
    mBuildTag = buildTag;
    mPatch = configuration.getPatch();
    mUrl = configuration.getJiraUrl();
    mUser = configuration.getJiraUser();
    mPassword = configuration.getJiraPassword();
    mJenkinsURL = configuration.getJenkinsURL();
    mLogsURL = configuration.getLogsURL();
  }

  void postComment(boolean error, int numTestsExecuted, SortedSet<String> failedTests,
      List<String> messages) {
    DefaultHttpClient httpClient = new DefaultHttpClient();
    try {
      BuildInfo buildInfo = formatBuildTag(mBuildTag);
      String buildTagForLogs = formatBuildTagForLogs(mBuildTag);
      List<String> comments = Lists.newArrayList();
      comments.add("");
      comments.add("");
      if(!failedTests.isEmpty()) {
        comments.add("{color:red}Overall{color}: -1 at least one tests failed");
      } else if(numTestsExecuted == 0) {
        comments.add("{color:red}Overall{color}: -1 no tests executed");
      } else if (error) {
        comments.add("{color:red}Overall{color}: -1 build exited with an error");
      } else {
        comments.add("{color:green}Overall{color}: +1 all checks pass");
      }
      comments.add("");
      if(!mPatch.isEmpty()) {
        comments.add("Here are the results of testing the latest attachment:");
        comments.add(mPatch);
      }
      comments.add("");
      if(numTestsExecuted > 0) {
        if (failedTests.isEmpty()) {
          comments.add(formatSuccess("+1 "+ numTestsExecuted + " tests passed"));
        } else {
          comments.add(formatError("-1 due to " + failedTests.size()
              + " failed/errored test(s), " + numTestsExecuted + " tests executed"));
          comments.add("*Failed tests:*");
          comments.add("{noformat}");
          comments.addAll(failedTests);
          comments.add("{noformat}");
        }
        comments.add("");
      }
      comments.add("Test results: " + mJenkinsURL + "/" +
          buildInfo.getFormattedBuildTag() + "/testReport");
      comments.add("Console output: " + mJenkinsURL + "/" +
          buildInfo.getFormattedBuildTag() + "/console");
      comments.add("Test logs: " + mLogsURL + buildTagForLogs);
      comments.add("");
      if(!messages.isEmpty()) {
        comments.add("Messages:");
        comments.add("{noformat}");
        comments.addAll(trimMessages(messages));
        comments.add("{noformat}");
        comments.add("");
      }
      comments.add("This message is automatically generated.");
      String attachmentId = parseAttachementId(mPatch);
      comments.add("");
      comments.add("ATTACHMENT ID: " + attachmentId +
          " - " + buildInfo.getBuildName());
      mLogger.info("Comment: " + Joiner.on("\n").join(comments));
      String body = Joiner.on("\n").join(comments);
      String url = String.format("%s/rest/api/2/issue/%s/comment", mUrl, mName);
      URL apiURL = new URL(mUrl);
      httpClient.getCredentialsProvider()
      .setCredentials(
          new AuthScope(apiURL.getHost(), apiURL.getPort(),
              AuthScope.ANY_REALM),
              new UsernamePasswordCredentials(mUser, mPassword));
      BasicHttpContext localcontext = new BasicHttpContext();
      localcontext.setAttribute("preemptive-auth", new BasicScheme());
      httpClient.addRequestInterceptor(new PreemptiveAuth(), 0);
      HttpPost request = new HttpPost(url);
      ObjectMapper mapper = new ObjectMapper();
      StringEntity params = new StringEntity(mapper.writeValueAsString(new Body(body)));
      request.addHeader("Content-Type", "application/json");
      request.setEntity(params);
      HttpResponse httpResponse = httpClient.execute(request, localcontext);
      StatusLine statusLine = httpResponse.getStatusLine();
      if (statusLine.getStatusCode() != 201) {
        throw new RuntimeException(statusLine.getStatusCode() + " "
            + statusLine.getReasonPhrase());
      }
      mLogger.info("JIRA Response Metadata: " + httpResponse);
    } catch (Exception e) {
      mLogger.error("Encountered error attempting to post comment to " + mName,
          e);
    } finally {
      httpClient.getConnectionManager().shutdown();
    }
  }
  static List<String> trimMessages(List<String> messages) {
    int size = messages.size();
    if(size > MAX_MESSAGES) {
      messages = messages.subList(size - MAX_MESSAGES, size);
      messages.add(0, TRIMMED_MESSAGE);
    }
    return messages;
  }
  @SuppressWarnings("unused")
  private static class Body {
    private String body;
    public Body() {

    }
    public Body(String body) {
      this.body = body;
    }
    public String getBody() {
      return body;
    }
    public void setBody(String body) {
      this.body = body;
    }
  }


  public static class BuildInfo {
    private String buildName;
    private String formattedBuildTag;

    public BuildInfo (String buildName, String formattedBuildTag) {
      this.buildName = buildName;
      this.formattedBuildTag = formattedBuildTag;
    }

    public String getBuildName() {
      return buildName;
    }

    public String getFormattedBuildTag() {
      return formattedBuildTag;
    }
  }

  /**
   * Hive-Build-123 to Hive-Build/123
   */
  @VisibleForTesting
  static BuildInfo formatBuildTag(String buildTag) {
    if(buildTag.contains("-")) {
      int lastDashIndex = buildTag.lastIndexOf("-");
      String buildName = buildTag.substring(0, lastDashIndex);
      String buildId = buildTag.substring(lastDashIndex + 1);
      String formattedBuildTag = buildName + "/" + buildId;
      return new BuildInfo(buildName, formattedBuildTag);
    }
    throw new IllegalArgumentException("Build tag '" + buildTag + "' must contain a -");
  }
  static String formatBuildTagForLogs(String buildTag) {
    if (buildTag.endsWith("/")) {
      return buildTag;
    } else {
      return buildTag + "/";
    }
  }
  private static String formatError(String msg) {
    return String.format("{color:red}ERROR:{color} %s", msg);
  }

  private static String formatSuccess(String msg) {
    return String.format("{color:green}SUCCESS:{color} %s", msg);
  }

  static class PreemptiveAuth implements HttpRequestInterceptor {

    public void process(final HttpRequest request, final HttpContext context)
        throws HttpException, IOException {
      AuthState authState = (AuthState) context.getAttribute(ClientContext.TARGET_AUTH_STATE);
      if (authState.getAuthScheme() == null) {
        AuthScheme authScheme = (AuthScheme) context.getAttribute("preemptive-auth");
        CredentialsProvider credsProvider = (CredentialsProvider) context.getAttribute(ClientContext.CREDS_PROVIDER);
        HttpHost targetHost = (HttpHost) context.getAttribute(ExecutionContext.HTTP_TARGET_HOST);
        if (authScheme != null) {
          Credentials creds = credsProvider.getCredentials(new AuthScope(
              targetHost.getHostName(), targetHost.getPort()));
          if (creds == null) {
            throw new HttpException(
                "No credentials for preemptive authentication");
          }
          authState.update(authScheme, creds);
        }
      }
    }
  }
  private static String parseAttachementId(String patch) {
    if(patch == null) {
      return "";
    }
    String result = FilenameUtils.getPathNoEndSeparator(patch.trim());
    if(result == null) {
      return "";
    }
    result = FilenameUtils.getName(result.trim());
    if(result == null) {
      return "";
    }
    return result.trim();
  }
  public static void main(String[] args) throws Exception {
    TestLogger logger = new TestLogger(System.err, TestLogger.LEVEL.TRACE);
    Map<String, String> context = Maps.newHashMap();
    context.put("jiraUrl", "https://issues.apache.org/jira");
    context.put("jiraUser", "hiveqa");
    context.put("jiraPassword", "password goes here");
    context.put("branch", "trunk");
    context.put("repository", "repo");
    context.put("repositoryName", "repoName");
    context.put("antArgs", "-Dsome=thing");
    context.put("logsURL", "http://ec2-174-129-184-35.compute-1.amazonaws.com/logs");
    TestConfiguration configuration = new TestConfiguration(new Context(context), logger);
    configuration.setJiraName("HIVE-4892");
    JIRAService service = new JIRAService(logger, configuration, "test-123");
    List<String> messages = Lists.newArrayList("msg1", "msg2");
    SortedSet<String> failedTests = Sets.newTreeSet(Collections.singleton("failed"));
    service.postComment(false, 5, failedTests, messages);
  }
}